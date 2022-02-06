import asyncio
from unittest import TestCase
from unittest.mock import MagicMock, PropertyMock, patch

from asyncio_taskpool import pool


EMPTY_LIST, EMPTY_DICT = [], {}


class BaseTaskPoolTestCase(TestCase):
    def setUp(self) -> None:
        self._pools = getattr(pool.BaseTaskPool, '_pools')

        # These three methods are called during initialization, so we mock them by default during setup
        self._add_pool_patcher = patch.object(pool.BaseTaskPool, '_add_pool')
        self.pool_size_patcher = patch.object(pool.BaseTaskPool, 'pool_size', new_callable=PropertyMock)
        self.__str___patcher = patch.object(pool.BaseTaskPool, '__str__')
        self.mock__add_pool = self._add_pool_patcher.start()
        self.mock_pool_size = self.pool_size_patcher.start()
        self.mock___str__ = self.__str___patcher.start()
        self.mock__add_pool.return_value = self.mock_idx = 123
        self.mock___str__.return_value = self.mock_str = 'foobar'

        # Test pool parameters:
        self.test_pool_size, self.test_pool_name = 420, 'test123'
        self.task_pool = pool.BaseTaskPool(pool_size=self.test_pool_size, name=self.test_pool_name)

    def tearDown(self) -> None:
        setattr(pool.TaskPool, '_pools', self._pools)
        self._add_pool_patcher.stop()
        self.pool_size_patcher.stop()
        self.__str___patcher.stop()

    def test__add_pool(self):
        self.assertListEqual(EMPTY_LIST, self._pools)
        self._add_pool_patcher.stop()
        output = pool.TaskPool._add_pool(self.task_pool)
        self.assertEqual(0, output)
        self.assertListEqual([self.task_pool], getattr(pool.TaskPool, '_pools'))

    def test_init(self):
        self.assertIsInstance(self.task_pool._enough_room, asyncio.locks.Semaphore)
        self.assertTrue(self.task_pool._open)
        self.assertEqual(0, self.task_pool._counter)
        self.assertDictEqual(EMPTY_DICT, self.task_pool._running)
        self.assertDictEqual(EMPTY_DICT, self.task_pool._cancelled)
        self.assertEqual(0, self.task_pool._ending)
        self.assertDictEqual(EMPTY_DICT, self.task_pool._ended)
        self.assertEqual(self.mock_idx, self.task_pool._idx)
        self.assertEqual(self.test_pool_name, self.task_pool._name)
        self.assertIsInstance(self.task_pool._all_tasks_known_flag, asyncio.locks.Event)
        self.assertTrue(self.task_pool._all_tasks_known_flag.is_set())
        self.mock__add_pool.assert_called_once_with(self.task_pool)
        self.mock_pool_size.assert_called_once_with(self.test_pool_size)
        self.mock___str__.assert_called_once_with()

    def test___str__(self):
        self.__str___patcher.stop()
        expected_str = f'{pool.BaseTaskPool.__name__}-{self.test_pool_name}'
        self.assertEqual(expected_str, str(self.task_pool))
        setattr(self.task_pool, '_name', None)
        expected_str = f'{pool.BaseTaskPool.__name__}-{self.task_pool._idx}'
        self.assertEqual(expected_str, str(self.task_pool))

    def test_pool_size(self):
        self.pool_size_patcher.stop()
        self.task_pool._pool_size = self.test_pool_size
        self.assertEqual(self.test_pool_size, self.task_pool.pool_size)

        with self.assertRaises(ValueError):
            self.task_pool.pool_size = -1

        self.task_pool.pool_size = new_size = 69
        self.assertEqual(new_size, self.task_pool._pool_size)

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
        self.assertEqual(self.task_pool._enough_room.locked(), self.task_pool.is_full)

    def test__task_name(self):
        i = 123
        self.assertEqual(f'{self.mock_str}_Task-{i}', self.task_pool._task_name(i))
