import asyncio
from asyncio.exceptions import CancelledError
from unittest import IsolatedAsyncioTestCase
from unittest.mock import PropertyMock, MagicMock, AsyncMock, patch, call

from asyncio_taskpool import pool, exceptions


EMPTY_LIST, EMPTY_DICT = [], {}
FOO, BAR = 'foo', 'bar'


class TestException(Exception):
    pass


class BaseTaskPoolTestCase(IsolatedAsyncioTestCase):
    log_lvl: int

    @classmethod
    def setUpClass(cls) -> None:
        cls.log_lvl = pool.log.level
        pool.log.setLevel(999)

    @classmethod
    def tearDownClass(cls) -> None:
        pool.log.setLevel(cls.log_lvl)

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
        self.assertDictEqual(EMPTY_DICT, self.task_pool._ended)
        self.assertEqual(0, self.task_pool._num_cancelled)
        self.assertEqual(0, self.task_pool._num_ended)
        self.assertEqual(self.mock_idx, self.task_pool._idx)
        self.assertEqual(self.test_pool_name, self.task_pool._name)
        self.assertListEqual(self.task_pool._before_gathering, EMPTY_LIST)
        self.assertIsInstance(self.task_pool._interrupt_flag, asyncio.locks.Event)
        self.assertFalse(self.task_pool._interrupt_flag.is_set())
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
        self.task_pool._open = FOO
        self.assertEqual(FOO, self.task_pool.is_open)

    def test_num_running(self):
        self.task_pool._running = ['foo', 'bar', 'baz']
        self.assertEqual(3, self.task_pool.num_running)

    def test_num_cancelled(self):
        self.task_pool._num_cancelled = 3
        self.assertEqual(3, self.task_pool.num_cancelled)

    def test_num_ended(self):
        self.task_pool._num_ended = 3
        self.assertEqual(3, self.task_pool.num_ended)

    def test_num_finished(self):
        self.task_pool._num_cancelled = cancelled = 69
        self.task_pool._num_ended = ended = 420
        self.task_pool._cancelled = mock_cancelled_dict = {1: 'foo', 2: 'bar'}
        self.assertEqual(ended - cancelled + len(mock_cancelled_dict), self.task_pool.num_finished)

    def test_is_full(self):
        self.assertEqual(self.task_pool._enough_room.locked(), self.task_pool.is_full)

    def test__task_name(self):
        i = 123
        self.assertEqual(f'{self.mock_str}_Task-{i}', self.task_pool._task_name(i))

    @patch.object(pool, 'execute_optional')
    @patch.object(pool.BaseTaskPool, '_task_name', return_value=FOO)
    async def test__task_cancellation(self, mock__task_name: MagicMock, mock_execute_optional: AsyncMock):
        task_id, mock_task, mock_callback = 1, MagicMock(), MagicMock()
        self.task_pool._num_cancelled = cancelled = 3
        self.task_pool._running[task_id] = mock_task
        self.assertIsNone(await self.task_pool._task_cancellation(task_id, mock_callback))
        self.assertNotIn(task_id, self.task_pool._running)
        self.assertEqual(mock_task, self.task_pool._cancelled[task_id])
        self.assertEqual(cancelled + 1, self.task_pool._num_cancelled)
        mock__task_name.assert_called_with(task_id)
        mock_execute_optional.assert_awaited_once_with(mock_callback, args=(task_id, ))

    @patch.object(pool, 'execute_optional')
    @patch.object(pool.BaseTaskPool, '_task_name', return_value=FOO)
    async def test__task_ending(self, mock__task_name: MagicMock, mock_execute_optional: AsyncMock):
        task_id, mock_task, mock_callback = 1, MagicMock(), MagicMock()
        self.task_pool._num_ended = ended = 3
        self.task_pool._enough_room._value = room = 123

        # End running task:
        self.task_pool._running[task_id] = mock_task
        self.assertIsNone(await self.task_pool._task_ending(task_id, mock_callback))
        self.assertNotIn(task_id, self.task_pool._running)
        self.assertEqual(mock_task, self.task_pool._ended[task_id])
        self.assertEqual(ended + 1, self.task_pool._num_ended)
        self.assertEqual(room + 1, self.task_pool._enough_room._value)
        mock__task_name.assert_called_with(task_id)
        mock_execute_optional.assert_awaited_once_with(mock_callback, args=(task_id, ))
        mock__task_name.reset_mock()
        mock_execute_optional.reset_mock()

        # End cancelled task:
        self.task_pool._cancelled[task_id] = self.task_pool._ended.pop(task_id)
        self.assertIsNone(await self.task_pool._task_ending(task_id, mock_callback))
        self.assertNotIn(task_id, self.task_pool._cancelled)
        self.assertEqual(mock_task, self.task_pool._ended[task_id])
        self.assertEqual(ended + 2, self.task_pool._num_ended)
        self.assertEqual(room + 2, self.task_pool._enough_room._value)
        mock__task_name.assert_called_with(task_id)
        mock_execute_optional.assert_awaited_once_with(mock_callback, args=(task_id, ))

    @patch.object(pool.BaseTaskPool, '_task_ending')
    @patch.object(pool.BaseTaskPool, '_task_cancellation')
    @patch.object(pool.BaseTaskPool, '_task_name', return_value=FOO)
    async def test__task_wrapper(self, mock__task_name: MagicMock,
                                 mock__task_cancellation: AsyncMock, mock__task_ending: AsyncMock):
        task_id = 42
        mock_cancel_cb, mock_end_cb = MagicMock(), MagicMock()
        mock_coroutine_func = AsyncMock(return_value=FOO, side_effect=CancelledError)

        # Cancelled during execution:
        mock_awaitable = mock_coroutine_func()
        output = await self.task_pool._task_wrapper(mock_awaitable, task_id,
                                                    end_callback=mock_end_cb, cancel_callback=mock_cancel_cb)
        self.assertIsNone(output)
        mock_coroutine_func.assert_awaited_once()
        mock__task_name.assert_called_with(task_id)
        mock__task_cancellation.assert_awaited_once_with(task_id, custom_callback=mock_cancel_cb)
        mock__task_ending.assert_awaited_once_with(task_id, custom_callback=mock_end_cb)

        mock_coroutine_func.reset_mock(side_effect=True)
        mock__task_name.reset_mock()
        mock__task_cancellation.reset_mock()
        mock__task_ending.reset_mock()

        # Not cancelled:
        mock_awaitable = mock_coroutine_func()
        output = await self.task_pool._task_wrapper(mock_awaitable, task_id,
                                                    end_callback=mock_end_cb, cancel_callback=mock_cancel_cb)
        self.assertEqual(FOO, output)
        mock_coroutine_func.assert_awaited_once()
        mock__task_name.assert_called_with(task_id)
        mock__task_cancellation.assert_not_awaited()
        mock__task_ending.assert_awaited_once_with(task_id, custom_callback=mock_end_cb)

    @patch.object(pool, 'create_task')
    @patch.object(pool.BaseTaskPool, '_task_wrapper', new_callable=MagicMock)
    @patch.object(pool.BaseTaskPool, '_task_name', return_value=FOO)
    @patch.object(pool.BaseTaskPool, 'is_open', new_callable=PropertyMock)
    async def test__start_task(self, mock_is_open: MagicMock, mock__task_name: MagicMock,
                               mock__task_wrapper: AsyncMock, mock_create_task: MagicMock):
        def reset_mocks() -> None:
            mock_is_open.reset_mock()
            mock__task_name.reset_mock()
            mock__task_wrapper.reset_mock()
            mock_create_task.reset_mock()

        mock_create_task.return_value = mock_task = MagicMock()
        mock__task_wrapper.return_value = mock_wrapped = MagicMock()
        mock_coroutine, mock_cancel_cb, mock_end_cb = AsyncMock(), MagicMock(), MagicMock()
        self.task_pool._counter = count = 123
        self.task_pool._enough_room._value = room = 123

        with self.assertRaises(exceptions.NotCoroutine):
            await self.task_pool._start_task(MagicMock(), end_callback=mock_end_cb, cancel_callback=mock_cancel_cb)
        self.assertEqual(count, self.task_pool._counter)
        self.assertNotIn(count, self.task_pool._running)
        self.assertEqual(room, self.task_pool._enough_room._value)
        mock_is_open.assert_not_called()
        mock__task_name.assert_not_called()
        mock__task_wrapper.assert_not_called()
        mock_create_task.assert_not_called()
        reset_mocks()

        mock_is_open.return_value = ignore_closed = False
        mock_awaitable = mock_coroutine()
        with self.assertRaises(exceptions.PoolIsClosed):
            await self.task_pool._start_task(mock_awaitable, ignore_closed,
                                             end_callback=mock_end_cb, cancel_callback=mock_cancel_cb)
        await mock_awaitable
        self.assertEqual(count, self.task_pool._counter)
        self.assertNotIn(count, self.task_pool._running)
        self.assertEqual(room, self.task_pool._enough_room._value)
        mock_is_open.assert_called_once_with()
        mock__task_name.assert_not_called()
        mock__task_wrapper.assert_not_called()
        mock_create_task.assert_not_called()
        reset_mocks()

        ignore_closed = True
        mock_awaitable = mock_coroutine()
        output = await self.task_pool._start_task(mock_awaitable, ignore_closed,
                                                  end_callback=mock_end_cb, cancel_callback=mock_cancel_cb)
        await mock_awaitable
        self.assertEqual(count, output)
        self.assertEqual(count + 1, self.task_pool._counter)
        self.assertEqual(mock_task, self.task_pool._running[count])
        self.assertEqual(room - 1, self.task_pool._enough_room._value)
        mock_is_open.assert_called_once_with()
        mock__task_name.assert_called_once_with(count)
        mock__task_wrapper.assert_called_once_with(mock_awaitable, count, mock_end_cb, mock_cancel_cb)
        mock_create_task.assert_called_once_with(mock_wrapped, name=FOO)
        reset_mocks()
        self.task_pool._counter = count
        self.task_pool._enough_room._value = room
        del self.task_pool._running[count]

        mock_awaitable = mock_coroutine()
        mock_create_task.side_effect = test_exception = TestException()
        with self.assertRaises(TestException) as e:
            await self.task_pool._start_task(mock_awaitable, ignore_closed,
                                             end_callback=mock_end_cb, cancel_callback=mock_cancel_cb)
            self.assertEqual(test_exception, e)
        await mock_awaitable
        self.assertEqual(count + 1, self.task_pool._counter)
        self.assertNotIn(count, self.task_pool._running)
        self.assertEqual(room, self.task_pool._enough_room._value)
        mock_is_open.assert_called_once_with()
        mock__task_name.assert_called_once_with(count)
        mock__task_wrapper.assert_called_once_with(mock_awaitable, count, mock_end_cb, mock_cancel_cb)
        mock_create_task.assert_called_once_with(mock_wrapped, name=FOO)

    @patch.object(pool.BaseTaskPool, '_task_name', return_value=FOO)
    def test__get_running_task(self, mock__task_name: MagicMock):
        task_id, mock_task = 555, MagicMock()
        self.task_pool._running[task_id] = mock_task
        output = self.task_pool._get_running_task(task_id)
        self.assertEqual(mock_task, output)

        self.task_pool._cancelled[task_id] = self.task_pool._running.pop(task_id)
        with self.assertRaises(exceptions.AlreadyCancelled):
            self.task_pool._get_running_task(task_id)
        mock__task_name.assert_called_once_with(task_id)
        mock__task_name.reset_mock()

        self.task_pool._ended[task_id] = self.task_pool._cancelled.pop(task_id)
        with self.assertRaises(exceptions.TaskEnded):
            self.task_pool._get_running_task(task_id)
        mock__task_name.assert_called_once_with(task_id)
        mock__task_name.reset_mock()

        del self.task_pool._ended[task_id]
        with self.assertRaises(exceptions.InvalidTaskID):
            self.task_pool._get_running_task(task_id)
        mock__task_name.assert_not_called()

    @patch.object(pool.BaseTaskPool, '_get_running_task')
    def test_cancel(self, mock__get_running_task: MagicMock):
        task_id1, task_id2, task_id3 = 1, 4, 9
        mock__get_running_task.return_value.cancel = mock_cancel = MagicMock()
        self.assertIsNone(self.task_pool.cancel(task_id1, task_id2, task_id3, msg=FOO))
        mock__get_running_task.assert_has_calls([call(task_id1), call(task_id2), call(task_id3)])
        mock_cancel.assert_has_calls([call(msg=FOO), call(msg=FOO), call(msg=FOO)])

    def test_cancel_all(self):
        mock_task1, mock_task2 = MagicMock(), MagicMock()
        self.task_pool._running = {1: mock_task1, 2: mock_task2}
        assert not self.task_pool._interrupt_flag.is_set()
        self.assertIsNone(self.task_pool.cancel_all(FOO))
        self.assertTrue(self.task_pool._interrupt_flag.is_set())
        mock_task1.cancel.assert_called_once_with(msg=FOO)
        mock_task2.cancel.assert_called_once_with(msg=FOO)

    async def test_flush(self):
        test_exception = TestException()
        mock_ended_func, mock_cancelled_func = AsyncMock(return_value=FOO), AsyncMock(side_effect=test_exception)
        self.task_pool._ended = {123: mock_ended_func()}
        self.task_pool._cancelled = {456: mock_cancelled_func()}
        self.task_pool._interrupt_flag.set()
        output = await self.task_pool.flush(return_exceptions=True)
        self.assertListEqual([FOO, test_exception], output)
        self.assertDictEqual(self.task_pool._ended, EMPTY_DICT)
        self.assertDictEqual(self.task_pool._cancelled, EMPTY_DICT)
        self.assertFalse(self.task_pool._interrupt_flag.is_set())

        self.task_pool._ended = {123: mock_ended_func()}
        self.task_pool._cancelled = {456: mock_cancelled_func()}
        output = await self.task_pool.flush(return_exceptions=True)
        self.assertListEqual([FOO, test_exception], output)
        self.assertDictEqual(self.task_pool._ended, EMPTY_DICT)
        self.assertDictEqual(self.task_pool._cancelled, EMPTY_DICT)

    def test_close(self):
        assert self.task_pool._open
        self.task_pool.close()
        self.assertFalse(self.task_pool._open)

    async def test_gather(self):
        test_exception = TestException()
        mock_ended_func, mock_cancelled_func = AsyncMock(return_value=FOO), AsyncMock(side_effect=test_exception)
        mock_running_func = AsyncMock(return_value=BAR)
        mock_queue_join = AsyncMock()
        self.task_pool._before_gathering = before_gather = [mock_queue_join()]
        self.task_pool._ended = ended = {123: mock_ended_func()}
        self.task_pool._cancelled = cancelled = {456: mock_cancelled_func()}
        self.task_pool._running = running = {789: mock_running_func()}
        self.task_pool._interrupt_flag.set()

        assert self.task_pool._open
        with self.assertRaises(exceptions.PoolStillOpen):
            await self.task_pool.gather()
        self.assertDictEqual(self.task_pool._ended, ended)
        self.assertDictEqual(self.task_pool._cancelled, cancelled)
        self.assertDictEqual(self.task_pool._running, running)
        self.assertListEqual(self.task_pool._before_gathering, before_gather)
        self.assertTrue(self.task_pool._interrupt_flag.is_set())

        self.task_pool._open = False

        def check_assertions(output) -> None:
            self.assertListEqual([FOO, test_exception, BAR], output)
            self.assertDictEqual(self.task_pool._ended, EMPTY_DICT)
            self.assertDictEqual(self.task_pool._cancelled, EMPTY_DICT)
            self.assertDictEqual(self.task_pool._running, EMPTY_DICT)
            self.assertListEqual(self.task_pool._before_gathering, EMPTY_LIST)
            self.assertFalse(self.task_pool._interrupt_flag.is_set())

        check_assertions(await self.task_pool.gather(return_exceptions=True))

        self.task_pool._before_gathering = [mock_queue_join()]
        self.task_pool._ended = {123: mock_ended_func()}
        self.task_pool._cancelled = {456: mock_cancelled_func()}
        self.task_pool._running = {789: mock_running_func()}
        check_assertions(await self.task_pool.gather(return_exceptions=True))
