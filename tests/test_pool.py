__author__ = "Daniil Fajnberg"
__copyright__ = "Copyright © 2022 Daniil Fajnberg"
__license__ = """GNU LGPLv3.0

This file is part of asyncio-taskpool.

asyncio-taskpool is free software: you can redistribute it and/or modify it under the terms of
version 3.0 of the GNU Lesser General Public License as published by the Free Software Foundation.

asyncio-taskpool is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 
See the GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License along with asyncio-taskpool. 
If not, see <https://www.gnu.org/licenses/>."""

__doc__ = """
Unittests for the `asyncio_taskpool.pool` module.
"""


import asyncio
from asyncio.exceptions import CancelledError
from asyncio.queues import Queue
from unittest import IsolatedAsyncioTestCase
from unittest.mock import PropertyMock, MagicMock, AsyncMock, patch, call
from typing import Type

from asyncio_taskpool import pool, exceptions


EMPTY_LIST, EMPTY_DICT = [], {}
FOO, BAR = 'foo', 'bar'


class TestException(Exception):
    pass


class CommonTestCase(IsolatedAsyncioTestCase):
    TEST_CLASS: Type[pool.BaseTaskPool] = pool.BaseTaskPool
    TEST_POOL_SIZE: int = 420
    TEST_POOL_NAME: str = 'test123'

    task_pool: pool.BaseTaskPool
    log_lvl: int

    @classmethod
    def setUpClass(cls) -> None:
        cls.log_lvl = pool.log.level
        pool.log.setLevel(999)

    @classmethod
    def tearDownClass(cls) -> None:
        pool.log.setLevel(cls.log_lvl)

    def get_task_pool_init_params(self) -> dict:
        return {'pool_size': self.TEST_POOL_SIZE, 'name': self.TEST_POOL_NAME}

    def setUp(self) -> None:
        self._pools = self.TEST_CLASS._pools
        # These three methods are called during initialization, so we mock them by default during setup:
        self._add_pool_patcher = patch.object(self.TEST_CLASS, '_add_pool')
        self.pool_size_patcher = patch.object(self.TEST_CLASS, 'pool_size', new_callable=PropertyMock)
        self.dunder_str_patcher = patch.object(self.TEST_CLASS, '__str__')
        self.mock__add_pool = self._add_pool_patcher.start()
        self.mock_pool_size = self.pool_size_patcher.start()
        self.mock___str__ = self.dunder_str_patcher.start()
        self.mock__add_pool.return_value = self.mock_idx = 123
        self.mock___str__.return_value = self.mock_str = 'foobar'

        self.task_pool = self.TEST_CLASS(**self.get_task_pool_init_params())

    def tearDown(self) -> None:
        self.TEST_CLASS._pools.clear()
        self._add_pool_patcher.stop()
        self.pool_size_patcher.stop()
        self.dunder_str_patcher.stop()


class BaseTaskPoolTestCase(CommonTestCase):

    def test__add_pool(self):
        self.assertListEqual(EMPTY_LIST, self._pools)
        self._add_pool_patcher.stop()
        output = pool.BaseTaskPool._add_pool(self.task_pool)
        self.assertEqual(0, output)
        self.assertListEqual([self.task_pool], pool.BaseTaskPool._pools)

    def test_init(self):
        self.assertIsInstance(self.task_pool._enough_room, asyncio.locks.Semaphore)
        self.assertFalse(self.task_pool._locked)
        self.assertEqual(0, self.task_pool._counter)
        self.assertDictEqual(EMPTY_DICT, self.task_pool._running)
        self.assertDictEqual(EMPTY_DICT, self.task_pool._cancelled)
        self.assertDictEqual(EMPTY_DICT, self.task_pool._ended)
        self.assertEqual(0, self.task_pool._num_cancelled)
        self.assertEqual(0, self.task_pool._num_ended)
        self.assertEqual(self.mock_idx, self.task_pool._idx)
        self.assertEqual(self.TEST_POOL_NAME, self.task_pool._name)
        self.assertListEqual(self.task_pool._before_gathering, EMPTY_LIST)
        self.assertIsInstance(self.task_pool._interrupt_flag, asyncio.locks.Event)
        self.assertFalse(self.task_pool._interrupt_flag.is_set())
        self.mock__add_pool.assert_called_once_with(self.task_pool)
        self.mock_pool_size.assert_called_once_with(self.TEST_POOL_SIZE)
        self.mock___str__.assert_called_once_with()

    def test___str__(self):
        self.dunder_str_patcher.stop()
        expected_str = f'{pool.BaseTaskPool.__name__}-{self.TEST_POOL_NAME}'
        self.assertEqual(expected_str, str(self.task_pool))
        self.task_pool._name = None
        expected_str = f'{pool.BaseTaskPool.__name__}-{self.task_pool._idx}'
        self.assertEqual(expected_str, str(self.task_pool))

    def test_pool_size(self):
        self.pool_size_patcher.stop()
        self.task_pool._pool_size = self.TEST_POOL_SIZE
        self.assertEqual(self.TEST_POOL_SIZE, self.task_pool.pool_size)

        with self.assertRaises(ValueError):
            self.task_pool.pool_size = -1

        self.task_pool.pool_size = new_size = 69
        self.assertEqual(new_size, self.task_pool._pool_size)

    def test_is_locked(self):
        self.task_pool._locked = FOO
        self.assertEqual(FOO, self.task_pool.is_locked)

    def test_lock(self):
        assert not self.task_pool._locked
        self.task_pool.lock()
        self.assertTrue(self.task_pool._locked)
        self.task_pool.lock()
        self.assertTrue(self.task_pool._locked)

    def test_unlock(self):
        self.task_pool._locked = True
        self.task_pool.unlock()
        self.assertFalse(self.task_pool._locked)
        self.task_pool.unlock()
        self.assertFalse(self.task_pool._locked)

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
    async def test__start_task(self, mock__task_name: MagicMock, mock__task_wrapper: AsyncMock,
                               mock_create_task: MagicMock):
        def reset_mocks() -> None:
            mock__task_name.reset_mock()
            mock__task_wrapper.reset_mock()
            mock_create_task.reset_mock()

        mock_create_task.return_value = mock_task = MagicMock()
        mock__task_wrapper.return_value = mock_wrapped = MagicMock()
        mock_coroutine, mock_cancel_cb, mock_end_cb = AsyncMock(), MagicMock(), MagicMock()
        self.task_pool._counter = count = 123
        self.task_pool._enough_room._value = room = 123

        def check_nothing_changed() -> None:
            self.assertEqual(count, self.task_pool._counter)
            self.assertNotIn(count, self.task_pool._running)
            self.assertEqual(room, self.task_pool._enough_room._value)
            mock__task_name.assert_not_called()
            mock__task_wrapper.assert_not_called()
            mock_create_task.assert_not_called()
            reset_mocks()

        with self.assertRaises(exceptions.NotCoroutine):
            await self.task_pool._start_task(MagicMock(), end_callback=mock_end_cb, cancel_callback=mock_cancel_cb)
        check_nothing_changed()

        self.task_pool._locked = True
        ignore_closed = False
        mock_awaitable = mock_coroutine()
        with self.assertRaises(exceptions.PoolIsLocked):
            await self.task_pool._start_task(mock_awaitable, ignore_closed,
                                             end_callback=mock_end_cb, cancel_callback=mock_cancel_cb)
        await mock_awaitable
        check_nothing_changed()

        ignore_closed = True
        mock_awaitable = mock_coroutine()
        output = await self.task_pool._start_task(mock_awaitable, ignore_closed,
                                                  end_callback=mock_end_cb, cancel_callback=mock_cancel_cb)
        await mock_awaitable
        self.assertEqual(count, output)
        self.assertEqual(count + 1, self.task_pool._counter)
        self.assertEqual(mock_task, self.task_pool._running[count])
        self.assertEqual(room - 1, self.task_pool._enough_room._value)
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

        assert not self.task_pool._locked
        with self.assertRaises(exceptions.PoolStillUnlocked):
            await self.task_pool.gather()
        self.assertDictEqual(self.task_pool._ended, ended)
        self.assertDictEqual(self.task_pool._cancelled, cancelled)
        self.assertDictEqual(self.task_pool._running, running)
        self.assertListEqual(self.task_pool._before_gathering, before_gather)
        self.assertTrue(self.task_pool._interrupt_flag.is_set())

        self.task_pool._locked = True

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


class TaskPoolTestCase(CommonTestCase):
    TEST_CLASS = pool.TaskPool
    task_pool: pool.TaskPool

    @patch.object(pool.TaskPool, '_start_task')
    async def test__apply_one(self, mock__start_task: AsyncMock):
        mock__start_task.return_value = expected_output = 12345
        mock_awaitable = MagicMock()
        mock_func = MagicMock(return_value=mock_awaitable)
        args, kwargs = (FOO, BAR), {'a': 1, 'b': 2}
        end_cb, cancel_cb = MagicMock(), MagicMock()
        output = await self.task_pool._apply_one(mock_func, args, kwargs, end_cb, cancel_cb)
        self.assertEqual(expected_output, output)
        mock_func.assert_called_once_with(*args, **kwargs)
        mock__start_task.assert_awaited_once_with(mock_awaitable, end_callback=end_cb, cancel_callback=cancel_cb)

        mock_func.reset_mock()
        mock__start_task.reset_mock()

        output = await self.task_pool._apply_one(mock_func, args, None, end_cb, cancel_cb)
        self.assertEqual(expected_output, output)
        mock_func.assert_called_once_with(*args)
        mock__start_task.assert_awaited_once_with(mock_awaitable, end_callback=end_cb, cancel_callback=cancel_cb)

    @patch.object(pool.TaskPool, '_apply_one')
    async def test_apply(self, mock__apply_one: AsyncMock):
        mock__apply_one.return_value = mock_id = 67890
        mock_func, num = MagicMock(), 3
        args, kwargs = (FOO, BAR), {'a': 1, 'b': 2}
        end_cb, cancel_cb = MagicMock(), MagicMock()
        expected_output = num * [mock_id]
        output = await self.task_pool.apply(mock_func, args, kwargs, num, end_cb, cancel_cb)
        self.assertEqual(expected_output, output)
        mock__apply_one.assert_has_awaits(num * [call(mock_func, args, kwargs, end_cb, cancel_cb)])

    async def test__queue_producer(self):
        mock_put = AsyncMock()
        mock_q = MagicMock(put=mock_put)
        args = (FOO, BAR, 123)
        assert not self.task_pool._interrupt_flag.is_set()
        self.assertIsNone(await self.task_pool._queue_producer(mock_q, args))
        mock_put.assert_has_awaits([call(arg) for arg in args])
        mock_put.reset_mock()
        self.task_pool._interrupt_flag.set()
        self.assertIsNone(await self.task_pool._queue_producer(mock_q, args))
        mock_put.assert_not_awaited()

    @patch.object(pool, 'partial')
    @patch.object(pool, 'star_function')
    @patch.object(pool.TaskPool, '_start_task')
    async def test__queue_consumer(self, mock__start_task: AsyncMock, mock_star_function: MagicMock,
                                   mock_partial: MagicMock):
        mock_partial.return_value = queue_callback = 'not really'
        mock_star_function.return_value = awaitable = 'totally an awaitable'
        q, arg = Queue(), 420.69
        q.put_nowait(arg)
        mock_func, stars = MagicMock(), 3
        mock_flag, end_cb, cancel_cb = MagicMock(), MagicMock(), MagicMock()
        self.assertIsNone(await self.task_pool._queue_consumer(q, mock_flag, mock_func, stars, end_cb, cancel_cb))
        self.assertTrue(q.empty())
        mock__start_task.assert_awaited_once_with(awaitable, ignore_lock=True,
                                                  end_callback=queue_callback, cancel_callback=cancel_cb)
        mock_star_function.assert_called_once_with(mock_func, arg, arg_stars=stars)
        mock_partial.assert_called_once_with(pool.TaskPool._queue_callback, self.task_pool,
                                             q=q, first_batch_started=mock_flag, func=mock_func, arg_stars=stars,
                                             end_callback=end_cb, cancel_callback=cancel_cb)
        mock__start_task.reset_mock()
        mock_star_function.reset_mock()
        mock_partial.reset_mock()

        self.assertIsNone(await self.task_pool._queue_consumer(q, mock_flag, mock_func, stars, end_cb, cancel_cb))
        self.assertTrue(q.empty())
        mock__start_task.assert_not_awaited()
        mock_star_function.assert_not_called()
        mock_partial.assert_not_called()

    @patch.object(pool, 'execute_optional')
    @patch.object(pool.TaskPool, '_queue_consumer')
    async def test__queue_callback(self, mock__queue_consumer: AsyncMock, mock_execute_optional: AsyncMock):
        task_id, mock_q = 420, MagicMock()
        mock_func, stars = MagicMock(), 3
        mock_wait = AsyncMock()
        mock_flag = MagicMock(wait=mock_wait)
        end_cb, cancel_cb = MagicMock(), MagicMock()
        self.assertIsNone(await self.task_pool._queue_callback(task_id, mock_q, mock_flag, mock_func, stars,
                                                               end_callback=end_cb, cancel_callback=cancel_cb))
        mock_wait.assert_awaited_once_with()
        mock__queue_consumer.assert_awaited_once_with(mock_q, mock_flag, mock_func, stars,
                                                      end_callback=end_cb, cancel_callback=cancel_cb)
        mock_execute_optional.assert_awaited_once_with(end_cb, args=(task_id,))

    @patch.object(pool, 'iter')
    @patch.object(pool, 'create_task')
    @patch.object(pool, 'join_queue', new_callable=MagicMock)
    @patch.object(pool.TaskPool, '_queue_producer', new_callable=MagicMock)
    async def test__set_up_args_queue(self, mock__queue_producer: MagicMock, mock_join_queue: MagicMock,
                                      mock_create_task: MagicMock, mock_iter: MagicMock):
        args, num_tasks = (FOO, BAR, 1, 2, 3), 2
        mock_join_queue.return_value = mock_join = 'awaitable'
        mock_iter.return_value = args_iter = iter(args)
        mock__queue_producer.return_value = mock_producer_coro = 'very awaitable'
        output_q = self.task_pool._set_up_args_queue(args, num_tasks)
        self.assertIsInstance(output_q, Queue)
        self.assertEqual(num_tasks, output_q.qsize())
        for arg in args[:num_tasks]:
            self.assertEqual(arg, output_q.get_nowait())
        self.assertTrue(output_q.empty())
        for arg in args[num_tasks:]:
            self.assertEqual(arg, next(args_iter))
        with self.assertRaises(StopIteration):
            next(args_iter)
        self.assertListEqual([mock_join], self.task_pool._before_gathering)
        mock_join_queue.assert_called_once_with(output_q)
        mock__queue_producer.assert_called_once_with(output_q, args_iter)
        mock_create_task.assert_called_once_with(mock_producer_coro)

        self.task_pool._before_gathering.clear()
        mock_join_queue.reset_mock()
        mock__queue_producer.reset_mock()
        mock_create_task.reset_mock()

        num_tasks = 6
        mock_iter.return_value = args_iter = iter(args)
        output_q = self.task_pool._set_up_args_queue(args, num_tasks)
        self.assertIsInstance(output_q, Queue)
        self.assertEqual(len(args), output_q.qsize())
        for arg in args:
            self.assertEqual(arg, output_q.get_nowait())
        self.assertTrue(output_q.empty())
        with self.assertRaises(StopIteration):
            next(args_iter)
        self.assertListEqual([mock_join], self.task_pool._before_gathering)
        mock_join_queue.assert_called_once_with(output_q)
        mock__queue_producer.assert_not_called()
        mock_create_task.assert_not_called()

    @patch.object(pool, 'Event')
    @patch.object(pool.TaskPool, '_queue_consumer')
    @patch.object(pool.TaskPool, '_set_up_args_queue')
    async def test__map(self, mock__set_up_args_queue: MagicMock, mock__queue_consumer: AsyncMock,
                        mock_event_cls: MagicMock):
        qsize = 4
        mock__set_up_args_queue.return_value = mock_q = MagicMock(qsize=MagicMock(return_value=qsize))
        mock_flag_set = MagicMock()
        mock_event_cls.return_value = mock_flag = MagicMock(set=mock_flag_set)

        mock_func, stars = MagicMock(), 3
        args_iter, group_size = (FOO, BAR, 1, 2, 3), 2
        end_cb, cancel_cb = MagicMock(), MagicMock()

        self.task_pool._locked = False
        with self.assertRaises(exceptions.PoolIsLocked):
            await self.task_pool._map(mock_func, args_iter, stars, group_size, end_cb, cancel_cb)
        mock__set_up_args_queue.assert_not_called()
        mock__queue_consumer.assert_not_awaited()
        mock_flag_set.assert_not_called()

        self.task_pool._locked = True
        self.assertIsNone(await self.task_pool._map(mock_func, args_iter, stars, group_size, end_cb, cancel_cb))
        mock__set_up_args_queue.assert_called_once_with(args_iter, group_size)
        mock__queue_consumer.assert_has_awaits(qsize * [call(mock_q, mock_flag, mock_func, arg_stars=stars,
                                                             end_callback=end_cb, cancel_callback=cancel_cb)])
        mock_flag_set.assert_called_once_with()

    @patch.object(pool.TaskPool, '_map')
    async def test_map(self, mock__map: AsyncMock):
        mock_func = MagicMock()
        arg_iter, group_size = (FOO, BAR, 1, 2, 3), 2
        end_cb, cancel_cb = MagicMock(), MagicMock()
        self.assertIsNone(await self.task_pool.map(mock_func, arg_iter, group_size, end_cb, cancel_cb))
        mock__map.assert_awaited_once_with(mock_func, arg_iter, arg_stars=0, group_size=group_size,
                                           end_callback=end_cb, cancel_callback=cancel_cb)

    @patch.object(pool.TaskPool, '_map')
    async def test_starmap(self, mock__map: AsyncMock):
        mock_func = MagicMock()
        args_iter, group_size = ([FOO], [BAR]), 2
        end_cb, cancel_cb = MagicMock(), MagicMock()
        self.assertIsNone(await self.task_pool.starmap(mock_func, args_iter, group_size, end_cb, cancel_cb))
        mock__map.assert_awaited_once_with(mock_func, args_iter, arg_stars=1, group_size=group_size,
                                           end_callback=end_cb, cancel_callback=cancel_cb)

    @patch.object(pool.TaskPool, '_map')
    async def test_doublestarmap(self, mock__map: AsyncMock):
        mock_func = MagicMock()
        kwargs_iter, group_size = [{'a': FOO}, {'a': BAR}], 2
        end_cb, cancel_cb = MagicMock(), MagicMock()
        self.assertIsNone(await self.task_pool.doublestarmap(mock_func, kwargs_iter, group_size, end_cb, cancel_cb))
        mock__map.assert_awaited_once_with(mock_func, kwargs_iter, arg_stars=2, group_size=group_size,
                                           end_callback=end_cb, cancel_callback=cancel_cb)


class SimpleTaskPoolTestCase(CommonTestCase):
    TEST_CLASS = pool.SimpleTaskPool
    task_pool: pool.SimpleTaskPool

    TEST_POOL_FUNC = AsyncMock(__name__=FOO)
    TEST_POOL_ARGS = (FOO, BAR)
    TEST_POOL_KWARGS = {'a': 1, 'b': 2}
    TEST_POOL_END_CB = MagicMock()
    TEST_POOL_CANCEL_CB = MagicMock()

    def get_task_pool_init_params(self) -> dict:
        return super().get_task_pool_init_params() | {
            'func': self.TEST_POOL_FUNC,
            'args': self.TEST_POOL_ARGS,
            'kwargs': self.TEST_POOL_KWARGS,
            'end_callback': self.TEST_POOL_END_CB,
            'cancel_callback': self.TEST_POOL_CANCEL_CB,
        }

    def setUp(self) -> None:
        self.base_class_init_patcher = patch.object(pool.BaseTaskPool, '__init__')
        self.base_class_init = self.base_class_init_patcher.start()
        super().setUp()

    def tearDown(self) -> None:
        self.base_class_init_patcher.stop()

    def test_init(self):
        self.assertEqual(self.TEST_POOL_FUNC, self.task_pool._func)
        self.assertEqual(self.TEST_POOL_ARGS, self.task_pool._args)
        self.assertEqual(self.TEST_POOL_KWARGS, self.task_pool._kwargs)
        self.assertEqual(self.TEST_POOL_END_CB, self.task_pool._end_callback)
        self.assertEqual(self.TEST_POOL_CANCEL_CB, self.task_pool._cancel_callback)
        self.base_class_init.assert_called_once_with(pool_size=self.TEST_POOL_SIZE, name=self.TEST_POOL_NAME)

        with self.assertRaises(exceptions.NotCoroutine):
            pool.SimpleTaskPool(MagicMock())

    def test_func_name(self):
        self.assertEqual(self.TEST_POOL_FUNC.__name__, self.task_pool.func_name)

    @patch.object(pool.SimpleTaskPool, '_start_task')
    async def test__start_one(self, mock__start_task: AsyncMock):
        mock__start_task.return_value = expected_output = 99
        self.task_pool._func = MagicMock(return_value=BAR)
        output = await self.task_pool._start_one()
        self.assertEqual(expected_output, output)
        self.task_pool._func.assert_called_once_with(*self.task_pool._args, **self.task_pool._kwargs)
        mock__start_task.assert_awaited_once_with(BAR, end_callback=self.task_pool._end_callback,
                                                  cancel_callback=self.task_pool._cancel_callback)

    @patch.object(pool.SimpleTaskPool, '_start_one')
    async def test_start(self, mock__start_one: AsyncMock):
        mock__start_one.return_value = FOO
        num = 5
        output = await self.task_pool.start(num)
        expected_output = num * [FOO]
        self.assertListEqual(expected_output, output)
        mock__start_one.assert_has_awaits(num * [call()])

    @patch.object(pool.SimpleTaskPool, 'cancel')
    def test_stop(self, mock_cancel: MagicMock):
        num = 2
        id1, id2, id3 = 5, 6, 7
        self.task_pool._running = {id1: FOO, id2: BAR, id3: FOO + BAR}
        output = self.task_pool.stop(num)
        expected_output = [id3, id2]
        self.assertEqual(expected_output, output)
        mock_cancel.assert_called_once_with(*expected_output)
        mock_cancel.reset_mock()

        num = 50
        output = self.task_pool.stop(num)
        expected_output = [id3, id2, id1]
        self.assertEqual(expected_output, output)
        mock_cancel.assert_called_once_with(*expected_output)

    @patch.object(pool.SimpleTaskPool, 'num_running', new_callable=PropertyMock)
    @patch.object(pool.SimpleTaskPool, 'stop')
    def test_stop_all(self, mock_stop: MagicMock, mock_num_running: MagicMock):
        mock_num_running.return_value = num = 9876
        mock_stop.return_value = expected_output = 'something'
        output = self.task_pool.stop_all()
        self.assertEqual(expected_output, output)
        mock_num_running.assert_called_once_with()
        mock_stop.assert_called_once_with(num)
