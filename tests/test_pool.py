import asyncio
from unittest import TestCase
from unittest.mock import MagicMock, PropertyMock, patch, call

from asyncio_taskpool import pool


EMPTY_LIST, EMPTY_DICT = [], {}


class BaseTaskPoolTestCase(TestCase):
    def setUp(self) -> None:
        self._pools = getattr(pool.BaseTaskPool, '_pools')

        # These three methods are called during initialization, so we mock them by default during setup
        self._add_pool_patcher = patch.object(pool.BaseTaskPool, '_add_pool')
        self._check_more_allowed_patcher = patch.object(pool.BaseTaskPool, '_check_more_allowed')
        self.__str___patcher = patch.object(pool.BaseTaskPool, '__str__')
        self.mock__add_pool = self._add_pool_patcher.start()
        self.mock__check_more_allowed = self._check_more_allowed_patcher.start()
        self.mock___str__ = self.__str___patcher.start()
        self.mock__add_pool.return_value = self.mock_idx = 123
        self.mock___str__.return_value = self.mock_str = 'foobar'

        # Test pool parameters:
        self.test_pool_size, self.test_pool_name = 420, 'test123'
        self.task_pool = pool.BaseTaskPool(pool_size=self.test_pool_size, name=self.test_pool_name)

    def tearDown(self) -> None:
        setattr(pool.TaskPool, '_pools', self._pools)
        self._add_pool_patcher.stop()
        self._check_more_allowed_patcher.stop()
        self.__str___patcher.stop()

    def test__add_pool(self):
        self.assertListEqual(EMPTY_LIST, self._pools)
        self._add_pool_patcher.stop()
        output = pool.TaskPool._add_pool(self.task_pool)
        self.assertEqual(0, output)
        self.assertListEqual([self.task_pool], getattr(pool.TaskPool, '_pools'))

    def test_init(self):
        self.assertEqual(self.test_pool_size, self.task_pool.pool_size)
        self.assertEqual(self.test_pool_name, self.task_pool._name)
        self.assertDictEqual(EMPTY_DICT, self.task_pool._running)
        self.assertDictEqual(EMPTY_DICT, self.task_pool._cancelled)
        self.assertEqual(0, self.task_pool._ending)
        self.assertDictEqual(EMPTY_DICT, self.task_pool._ended)
        self.assertEqual(self.mock_idx, self.task_pool._idx)
        self.assertIsInstance(self.task_pool._all_tasks_known_flag, asyncio.locks.Event)
        self.assertTrue(self.task_pool._all_tasks_known_flag.is_set())
        self.assertIsInstance(self.task_pool._more_allowed_flag, asyncio.locks.Event)
        self.mock__add_pool.assert_called_once_with(self.task_pool)
        self.mock__check_more_allowed.assert_called_once_with()
        self.mock___str__.assert_called_once_with()

    def test___str__(self):
        self.__str___patcher.stop()
        expected_str = f'{pool.BaseTaskPool.__name__}-{self.test_pool_name}'
        self.assertEqual(expected_str, str(self.task_pool))
        setattr(self.task_pool, '_name', None)
        expected_str = f'{pool.BaseTaskPool.__name__}-{self.task_pool._idx}'
        self.assertEqual(expected_str, str(self.task_pool))

    def test_is_open(self):
        self.task_pool._open = foo = 'foo'
        self.assertEqual(foo, self.task_pool.is_open)

    def test_num_running(self):
        self.task_pool._running = ['foo', 'bar', 'baz']
        self.assertEqual(3, self.task_pool.num_running)

    def test_num_cancelled(self):
        self.task_pool._cancelled = ['foo', 'bar', 'baz']
        self.assertEqual(3, self.task_pool.num_cancelled)

    def test_num_ended(self):
        self.task_pool._ended = ['foo', 'bar', 'baz']
        self.assertEqual(3, self.task_pool.num_ended)

    @patch.object(pool.BaseTaskPool, 'num_ended', new_callable=PropertyMock)
    @patch.object(pool.BaseTaskPool, 'num_cancelled', new_callable=PropertyMock)
    def test_num_finished(self, mock_num_cancelled: MagicMock, mock_num_ended: MagicMock):
        mock_num_cancelled.return_value = cancelled = 69
        mock_num_ended.return_value = ended = 420
        self.task_pool._ending = mock_ending = 2
        self.assertEqual(ended - cancelled + mock_ending, self.task_pool.num_finished)
        mock_num_cancelled.assert_called_once_with()
        mock_num_ended.assert_called_once_with()

    def test_is_full(self):
        self.assertEqual(not self.task_pool._more_allowed_flag.is_set(), self.task_pool.is_full)

    @patch.object(pool.BaseTaskPool, 'num_running', new_callable=PropertyMock)
    @patch.object(pool.BaseTaskPool, 'is_full', new_callable=PropertyMock)
    def test__check_more_allowed(self, mock_is_full: MagicMock, mock_num_running: MagicMock):
        def reset_mocks():
            mock_is_full.reset_mock()
            mock_num_running.reset_mock()
        self._check_more_allowed_patcher.stop()

        # Just reaching limit, we expect flag to become unset:
        mock_is_full.return_value = False
        mock_num_running.return_value = 420
        self.task_pool._more_allowed_flag.clear()
        self.task_pool._check_more_allowed()
        self.assertFalse(self.task_pool._more_allowed_flag.is_set())
        mock_is_full.assert_has_calls([call(), call()])
        mock_num_running.assert_called_once_with()
        reset_mocks()

        # Already at limit, we expect nothing to change:
        mock_is_full.return_value = True
        self.task_pool._check_more_allowed()
        self.assertFalse(self.task_pool._more_allowed_flag.is_set())
        mock_is_full.assert_has_calls([call(), call()])
        mock_num_running.assert_called_once_with()
        reset_mocks()

        # Just finished a task, we expect flag to become set:
        mock_num_running.return_value = 419
        self.task_pool._check_more_allowed()
        self.assertTrue(self.task_pool._more_allowed_flag.is_set())
        mock_is_full.assert_called_once_with()
        mock_num_running.assert_called_once_with()
        reset_mocks()

        # In this state we expect the flag to remain unchanged change:
        mock_is_full.return_value = False
        self.task_pool._check_more_allowed()
        self.assertTrue(self.task_pool._more_allowed_flag.is_set())
        mock_is_full.assert_has_calls([call(), call()])
        mock_num_running.assert_called_once_with()

    def test__task_name(self):
        i = 123
        self.assertEqual(f'{self.mock_str}_Task-{i}', self.task_pool._task_name(i))
