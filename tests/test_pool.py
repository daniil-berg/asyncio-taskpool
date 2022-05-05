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

from asyncio.exceptions import CancelledError
from asyncio.locks import Event, Semaphore
from unittest import IsolatedAsyncioTestCase
from unittest.mock import PropertyMock, MagicMock, AsyncMock, patch, call
from typing import Type

from asyncio_taskpool import pool, exceptions


EMPTY_LIST, EMPTY_DICT, EMPTY_SET = [], {}, set()
FOO, BAR, BAZ = 'foo', 'bar', 'baz'


class TestException(Exception):
    pass


class CommonTestCase(IsolatedAsyncioTestCase):
    TEST_CLASS: Type[pool.BaseTaskPool] = pool.BaseTaskPool
    TEST_POOL_SIZE: int = 420
    TEST_POOL_NAME: str = 'test123'

    task_pool: pool.BaseTaskPool
    log_lvl: int

    def get_task_pool_init_params(self) -> dict:
        return {'pool_size': self.TEST_POOL_SIZE, 'name': self.TEST_POOL_NAME}

    def setUp(self) -> None:
        self.log_lvl = pool.log.level
        pool.log.setLevel(999)
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
        pool.log.setLevel(self.log_lvl)


class BaseTaskPoolTestCase(CommonTestCase):

    def test__add_pool(self):
        self.assertListEqual(EMPTY_LIST, self._pools)
        self._add_pool_patcher.stop()
        output = pool.BaseTaskPool._add_pool(self.task_pool)
        self.assertEqual(0, output)
        self.assertListEqual([self.task_pool], pool.BaseTaskPool._pools)

    def test_init(self):
        self.assertEqual(0, self.task_pool._num_started)

        self.assertFalse(self.task_pool._locked)
        self.assertIsInstance(self.task_pool._closed, Event)
        self.assertFalse(self.task_pool._closed.is_set())
        self.assertEqual(self.TEST_POOL_NAME, self.task_pool._name)

        self.assertDictEqual(EMPTY_DICT, self.task_pool._tasks_running)
        self.assertDictEqual(EMPTY_DICT, self.task_pool._tasks_cancelled)
        self.assertDictEqual(EMPTY_DICT, self.task_pool._tasks_ended)

        self.assertIsInstance(self.task_pool._enough_room, Semaphore)
        self.assertDictEqual(EMPTY_DICT, self.task_pool._task_groups)

        self.assertDictEqual(EMPTY_DICT, self.task_pool._group_meta_tasks_running)
        self.assertSetEqual(EMPTY_SET, self.task_pool._meta_tasks_cancelled)

        self.assertEqual(self.mock_idx, self.task_pool._idx)

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
        self.task_pool._enough_room._value = self.TEST_POOL_SIZE
        self.assertEqual(self.TEST_POOL_SIZE, self.task_pool.pool_size)

        with self.assertRaises(ValueError):
            self.task_pool.pool_size = -1

        self.task_pool.pool_size = new_size = 69
        self.assertEqual(new_size, self.task_pool._enough_room._value)

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
        self.task_pool._tasks_running = {1: FOO, 2: BAR, 3: BAZ}
        self.assertEqual(3, self.task_pool.num_running)

    def test_num_cancelled(self):
        self.task_pool._tasks_cancelled = {1: FOO, 2: BAR, 3: BAZ}
        self.assertEqual(3, self.task_pool.num_cancelled)

    def test_num_ended(self):
        self.task_pool._tasks_ended = {1: FOO, 2: BAR, 3: BAZ}
        self.assertEqual(3, self.task_pool.num_ended)

    def test_is_full(self):
        self.assertEqual(self.task_pool._enough_room.locked(), self.task_pool.is_full)

    def test_get_group_ids(self):
        group_name, ids = 'abcdef', [1, 2, 3]
        self.task_pool._task_groups[group_name] = MagicMock(__iter__=lambda _: iter(ids))
        self.assertEqual(set(ids), self.task_pool.get_group_ids(group_name))
        with self.assertRaises(exceptions.InvalidGroupName):
            self.task_pool.get_group_ids(group_name, 'something else')

    async def test__check_start(self):
        self.task_pool._closed.set()
        mock_coroutine, mock_coroutine_function = AsyncMock()(), AsyncMock()
        try:
            with self.assertRaises(AssertionError):
                self.task_pool._check_start(awaitable=None, function=None)
            with self.assertRaises(AssertionError):
                self.task_pool._check_start(awaitable=mock_coroutine, function=mock_coroutine_function)
            with self.assertRaises(exceptions.NotCoroutine):
                self.task_pool._check_start(awaitable=mock_coroutine_function, function=None)
            with self.assertRaises(exceptions.NotCoroutine):
                self.task_pool._check_start(awaitable=None, function=mock_coroutine)
            with self.assertRaises(exceptions.PoolIsClosed):
                self.task_pool._check_start(awaitable=mock_coroutine, function=None)
            self.task_pool._closed.clear()
            self.task_pool._locked = True
            with self.assertRaises(exceptions.PoolIsLocked):
                self.task_pool._check_start(awaitable=mock_coroutine, function=None, ignore_lock=False)
            self.assertIsNone(self.task_pool._check_start(awaitable=mock_coroutine, function=None, ignore_lock=True))
        finally:
            await mock_coroutine

    def test__task_name(self):
        i = 123
        self.assertEqual(f'{self.mock_str}_Task-{i}', self.task_pool._task_name(i))

    @patch.object(pool, 'execute_optional')
    @patch.object(pool.BaseTaskPool, '_task_name', return_value=FOO)
    async def test__task_cancellation(self, mock__task_name: MagicMock, mock_execute_optional: AsyncMock):
        task_id, mock_task, mock_callback = 1, MagicMock(), MagicMock()
        self.task_pool._tasks_running[task_id] = mock_task
        self.assertIsNone(await self.task_pool._task_cancellation(task_id, mock_callback))
        self.assertNotIn(task_id, self.task_pool._tasks_running)
        self.assertEqual(mock_task, self.task_pool._tasks_cancelled[task_id])
        mock__task_name.assert_called_with(task_id)
        mock_execute_optional.assert_awaited_once_with(mock_callback, args=(task_id, ))

    @patch.object(pool, 'execute_optional')
    @patch.object(pool.BaseTaskPool, '_task_name', return_value=FOO)
    async def test__task_ending(self, mock__task_name: MagicMock, mock_execute_optional: AsyncMock):
        task_id, mock_task, mock_callback = 1, MagicMock(), MagicMock()
        self.task_pool._enough_room._value = room = 123

        # End running task:
        self.task_pool._tasks_running[task_id] = mock_task
        self.assertIsNone(await self.task_pool._task_ending(task_id, mock_callback))
        self.assertNotIn(task_id, self.task_pool._tasks_running)
        self.assertEqual(mock_task, self.task_pool._tasks_ended[task_id])
        self.assertEqual(room + 1, self.task_pool._enough_room._value)
        mock__task_name.assert_called_with(task_id)
        mock_execute_optional.assert_awaited_once_with(mock_callback, args=(task_id, ))
        mock__task_name.reset_mock()
        mock_execute_optional.reset_mock()

        # End cancelled task:
        self.task_pool._tasks_cancelled[task_id] = self.task_pool._tasks_ended.pop(task_id)
        self.assertIsNone(await self.task_pool._task_ending(task_id, mock_callback))
        self.assertNotIn(task_id, self.task_pool._tasks_cancelled)
        self.assertEqual(mock_task, self.task_pool._tasks_ended[task_id])
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
    @patch.object(pool, 'TaskGroupRegister')
    @patch.object(pool.BaseTaskPool, '_check_start')
    async def test__start_task(self, mock__check_start: MagicMock, mock_reg_cls: MagicMock, mock__task_name: MagicMock,
                               mock__task_wrapper: AsyncMock, mock_create_task: MagicMock):
        mock_group_reg = set_up_mock_group_register(mock_reg_cls)
        mock_create_task.return_value = mock_task = MagicMock()
        mock__task_wrapper.return_value = mock_wrapped = MagicMock()
        mock_coroutine, mock_cancel_cb, mock_end_cb = MagicMock(), MagicMock(), MagicMock()
        self.task_pool._num_started = count = 123
        self.task_pool._enough_room._value = room = 123
        group_name, ignore_lock = 'testgroup', True
        output = await self.task_pool._start_task(mock_coroutine, group_name=group_name, ignore_lock=ignore_lock,
                                                  end_callback=mock_end_cb, cancel_callback=mock_cancel_cb)
        self.assertEqual(count, output)
        mock__check_start.assert_called_once_with(awaitable=mock_coroutine, ignore_lock=ignore_lock)
        self.assertEqual(room - 1, self.task_pool._enough_room._value)
        self.assertEqual(mock_group_reg, self.task_pool._task_groups[group_name])
        mock_reg_cls.assert_called_once_with()
        mock_group_reg.__aenter__.assert_awaited_once_with()
        mock_group_reg.add.assert_called_once_with(count)
        mock__task_name.assert_called_once_with(count)
        mock__task_wrapper.assert_called_once_with(mock_coroutine, count, mock_end_cb, mock_cancel_cb)
        mock_create_task.assert_called_once_with(coro=mock_wrapped, name=FOO)
        self.assertEqual(mock_task, self.task_pool._tasks_running[count])
        mock_group_reg.__aexit__.assert_awaited_once()

    @patch.object(pool.BaseTaskPool, '_task_name', return_value=FOO)
    def test__get_running_task(self, mock__task_name: MagicMock):
        task_id, mock_task = 555, MagicMock()
        self.task_pool._tasks_running[task_id] = mock_task
        output = self.task_pool._get_running_task(task_id)
        self.assertEqual(mock_task, output)

        self.task_pool._tasks_cancelled[task_id] = self.task_pool._tasks_running.pop(task_id)
        with self.assertRaises(exceptions.AlreadyCancelled):
            self.task_pool._get_running_task(task_id)
        mock__task_name.assert_called_once_with(task_id)
        mock__task_name.reset_mock()

        self.task_pool._tasks_ended[task_id] = self.task_pool._tasks_cancelled.pop(task_id)
        with self.assertRaises(exceptions.TaskEnded):
            self.task_pool._get_running_task(task_id)
        mock__task_name.assert_called_once_with(task_id)
        mock__task_name.reset_mock()

        del self.task_pool._tasks_ended[task_id]
        with self.assertRaises(exceptions.InvalidTaskID):
            self.task_pool._get_running_task(task_id)
        mock__task_name.assert_not_called()

    @patch('warnings.warn')
    def test__get_cancel_kw(self, mock_warn: MagicMock):
        msg = None
        self.assertDictEqual(EMPTY_DICT, pool.BaseTaskPool._get_cancel_kw(msg))
        mock_warn.assert_not_called()

        msg = 'something'
        with patch.object(pool, 'PYTHON_BEFORE_39', new=True):
            self.assertDictEqual(EMPTY_DICT, pool.BaseTaskPool._get_cancel_kw(msg))
            mock_warn.assert_called_once()
        mock_warn.reset_mock()

        with patch.object(pool, 'PYTHON_BEFORE_39', new=False):
            self.assertDictEqual({'msg': msg}, pool.BaseTaskPool._get_cancel_kw(msg))
            mock_warn.assert_not_called()

    @patch.object(pool.BaseTaskPool, '_get_cancel_kw')
    @patch.object(pool.BaseTaskPool, '_get_running_task')
    def test_cancel(self, mock__get_running_task: MagicMock, mock__get_cancel_kw: MagicMock):
        mock__get_cancel_kw.return_value = fake_cancel_kw = {'a': 10, 'b': 20}
        task_id1, task_id2, task_id3 = 1, 4, 9
        mock__get_running_task.return_value.cancel = mock_cancel = MagicMock()
        self.assertIsNone(self.task_pool.cancel(task_id1, task_id2, task_id3, msg=FOO))
        mock__get_running_task.assert_has_calls([call(task_id1), call(task_id2), call(task_id3)])
        mock__get_cancel_kw.assert_called_once_with(FOO)
        mock_cancel.assert_has_calls(3 * [call(**fake_cancel_kw)])

    def test__cancel_group_meta_tasks(self):
        mock_task1, mock_task2 = MagicMock(), MagicMock()
        self.task_pool._group_meta_tasks_running[BAR] = {mock_task1, mock_task2}
        self.assertIsNone(self.task_pool._cancel_group_meta_tasks(FOO))
        self.assertDictEqual({BAR: {mock_task1, mock_task2}}, self.task_pool._group_meta_tasks_running)
        self.assertSetEqual(EMPTY_SET, self.task_pool._meta_tasks_cancelled)
        mock_task1.cancel.assert_not_called()
        mock_task2.cancel.assert_not_called()

        self.assertIsNone(self.task_pool._cancel_group_meta_tasks(BAR))
        self.assertDictEqual(EMPTY_DICT, self.task_pool._group_meta_tasks_running)
        self.assertSetEqual({mock_task1, mock_task2}, self.task_pool._meta_tasks_cancelled)
        mock_task1.cancel.assert_called_once_with()
        mock_task2.cancel.assert_called_once_with()

    @patch.object(pool.BaseTaskPool, '_cancel_group_meta_tasks')
    def test__cancel_and_remove_all_from_group(self, mock__cancel_group_meta_tasks: MagicMock):
        kw = {BAR: 10, BAZ: 20}
        task_id = 555
        mock_cancel = MagicMock()

        def add_mock_task_to_running(_):
            self.task_pool._tasks_running[task_id] = MagicMock(cancel=mock_cancel)
        # We add the fake task to the `_tasks_running` dictionary as a side effect of calling the mocked method,
        # to verify that it is called first, before the cancellation loop starts.
        mock__cancel_group_meta_tasks.side_effect = add_mock_task_to_running

        class MockRegister(set, MagicMock):
            pass
        self.assertIsNone(self.task_pool._cancel_and_remove_all_from_group(' ', MockRegister({task_id, 'x'}), **kw))
        mock_cancel.assert_called_once_with(**kw)

    @patch.object(pool.BaseTaskPool, '_get_cancel_kw')
    @patch.object(pool.BaseTaskPool, '_cancel_and_remove_all_from_group')
    def test_cancel_group(self, mock__cancel_and_remove_all_from_group: MagicMock, mock__get_cancel_kw: MagicMock):
        mock__get_cancel_kw.return_value = fake_cancel_kw = {'a': 10, 'b': 20}
        self.task_pool._task_groups[FOO] = mock_group_reg = MagicMock()
        with self.assertRaises(exceptions.InvalidGroupName):
            self.task_pool.cancel_group(BAR)
        mock__cancel_and_remove_all_from_group.assert_not_called()
        self.assertIsNone(self.task_pool.cancel_group(FOO, msg=BAR))
        self.assertDictEqual(EMPTY_DICT, self.task_pool._task_groups)
        mock__get_cancel_kw.assert_called_once_with(BAR)
        mock__cancel_and_remove_all_from_group.assert_called_once_with(FOO, mock_group_reg, **fake_cancel_kw)

    @patch.object(pool.BaseTaskPool, '_get_cancel_kw')
    @patch.object(pool.BaseTaskPool, '_cancel_and_remove_all_from_group')
    def test_cancel_all(self, mock__cancel_and_remove_all_from_group: MagicMock, mock__get_cancel_kw: MagicMock):
        mock__get_cancel_kw.return_value = fake_cancel_kw = {'a': 10, 'b': 20}
        mock_group_reg = MagicMock()
        self.task_pool._task_groups = {FOO: mock_group_reg, BAR: mock_group_reg}
        self.assertIsNone(self.task_pool.cancel_all(BAZ))
        mock__get_cancel_kw.assert_called_once_with(BAZ)
        mock__cancel_and_remove_all_from_group.assert_has_calls([
            call(BAR, mock_group_reg, **fake_cancel_kw),
            call(FOO, mock_group_reg, **fake_cancel_kw)
        ])

    def test__pop_ended_meta_tasks(self):
        mock_task, mock_done_task1 = MagicMock(done=lambda: False), MagicMock(done=lambda: True)
        self.task_pool._group_meta_tasks_running[FOO] = {mock_task, mock_done_task1}
        mock_done_task2, mock_done_task3 = MagicMock(done=lambda: True), MagicMock(done=lambda: True)
        self.task_pool._group_meta_tasks_running[BAR] = {mock_done_task2, mock_done_task3}
        expected_output = {mock_done_task1, mock_done_task2, mock_done_task3}
        output = self.task_pool._pop_ended_meta_tasks()
        self.assertSetEqual(expected_output, output)
        self.assertDictEqual({FOO: {mock_task}}, self.task_pool._group_meta_tasks_running)

    @patch.object(pool.BaseTaskPool, '_pop_ended_meta_tasks')
    async def test_flush(self, mock__pop_ended_meta_tasks: MagicMock):
        # Meta tasks:
        mock_ended_meta_task = AsyncMock()
        mock__pop_ended_meta_tasks.return_value = {mock_ended_meta_task()}
        mock_cancelled_meta_task = AsyncMock(side_effect=CancelledError)
        self.task_pool._meta_tasks_cancelled = {mock_cancelled_meta_task()}
        # Actual tasks:
        mock_ended_func, mock_cancelled_func = AsyncMock(), AsyncMock(side_effect=Exception)
        self.task_pool._tasks_ended = {123: mock_ended_func()}
        self.task_pool._tasks_cancelled = {456: mock_cancelled_func()}

        self.assertIsNone(await self.task_pool.flush(return_exceptions=True))

        # Meta tasks:
        mock__pop_ended_meta_tasks.assert_called_once_with()
        mock_ended_meta_task.assert_awaited_once_with()
        mock_cancelled_meta_task.assert_awaited_once_with()
        self.assertSetEqual(EMPTY_SET, self.task_pool._meta_tasks_cancelled)
        # Actual tasks:
        mock_ended_func.assert_awaited_once_with()
        mock_cancelled_func.assert_awaited_once_with()
        self.assertDictEqual(EMPTY_DICT, self.task_pool._tasks_ended)
        self.assertDictEqual(EMPTY_DICT, self.task_pool._tasks_cancelled)

    @patch.object(pool.BaseTaskPool, 'lock')
    async def test_gather_and_close(self, mock_lock: MagicMock):
        # Meta tasks:
        mock_meta_task1, mock_meta_task2 = AsyncMock(), AsyncMock()
        self.task_pool._group_meta_tasks_running = {FOO: {mock_meta_task1()}, BAR: {mock_meta_task2()}}
        mock_cancelled_meta_task = AsyncMock(side_effect=CancelledError)
        self.task_pool._meta_tasks_cancelled = {mock_cancelled_meta_task()}
        # Actual tasks:
        mock_running_func = AsyncMock()
        mock_ended_func, mock_cancelled_func = AsyncMock(), AsyncMock(side_effect=Exception)
        self.task_pool._tasks_ended = {123: mock_ended_func()}
        self.task_pool._tasks_cancelled = {456: mock_cancelled_func()}
        self.task_pool._tasks_running = {789: mock_running_func()}

        self.assertIsNone(await self.task_pool.gather_and_close(return_exceptions=True))

        mock_lock.assert_called_once_with()
        # Meta tasks:
        mock_meta_task1.assert_awaited_once_with()
        mock_meta_task2.assert_awaited_once_with()
        mock_cancelled_meta_task.assert_awaited_once_with()
        self.assertDictEqual(EMPTY_DICT, self.task_pool._group_meta_tasks_running)
        self.assertSetEqual(EMPTY_SET, self.task_pool._meta_tasks_cancelled)
        # Actual tasks:
        mock_ended_func.assert_awaited_once_with()
        mock_cancelled_func.assert_awaited_once_with()
        mock_running_func.assert_awaited_once_with()
        self.assertDictEqual(EMPTY_DICT, self.task_pool._tasks_ended)
        self.assertDictEqual(EMPTY_DICT, self.task_pool._tasks_cancelled)
        self.assertDictEqual(EMPTY_DICT, self.task_pool._tasks_running)
        self.assertTrue(self.task_pool._closed.is_set())

    async def test_until_closed(self):
        self.task_pool._closed = MagicMock(wait=AsyncMock(return_value=FOO))
        output = await self.task_pool.until_closed()
        self.assertEqual(FOO, output)
        self.task_pool._closed.wait.assert_awaited_once_with()


class TaskPoolTestCase(CommonTestCase):
    TEST_CLASS = pool.TaskPool
    task_pool: pool.TaskPool

    def test__generate_group_name(self):
        prefix, func = 'x y z', AsyncMock(__name__=BAR)
        base_name = f'{prefix}-{BAR}-group'
        self.task_pool._task_groups = {
            f'{base_name}-0': MagicMock(),
            f'{base_name}-1': MagicMock(),
            f'{base_name}-100': MagicMock(),
        }
        expected_output = f'{base_name}-2'
        output = self.task_pool._generate_group_name(prefix, func)
        self.assertEqual(expected_output, output)

    @patch.object(pool.TaskPool, '_start_task')
    async def test__apply_spawner(self, mock__start_task: AsyncMock):
        grp_name = FOO + BAR
        mock_awaitable1, mock_awaitable2 = object(), object()
        mock_func = MagicMock(side_effect=[mock_awaitable1, Exception(), mock_awaitable2], __name__='func')
        args, kw, num = (FOO, BAR), {'a': 1, 'b': 2}, 3
        end_cb, cancel_cb = MagicMock(), MagicMock()
        self.assertIsNone(await self.task_pool._apply_spawner(grp_name, mock_func, args, kw, num, end_cb, cancel_cb))
        mock_func.assert_has_calls(num * [call(*args, **kw)])
        mock__start_task.assert_has_awaits([
            call(mock_awaitable1, group_name=grp_name, end_callback=end_cb, cancel_callback=cancel_cb),
            call(mock_awaitable2, group_name=grp_name, end_callback=end_cb, cancel_callback=cancel_cb),
        ])

        mock_func.reset_mock(side_effect=True)
        mock__start_task.reset_mock()

        # Simulate cancellation while the second task is being started.
        mock__start_task.side_effect = [None, CancelledError, None]
        mock_coroutine_to_close = MagicMock()
        mock_func.side_effect = [mock_awaitable1, mock_coroutine_to_close, 'never called']
        self.assertIsNone(await self.task_pool._apply_spawner(grp_name, mock_func, args, None, num, end_cb, cancel_cb))
        mock_func.assert_has_calls(2 * [call(*args)])
        mock__start_task.assert_has_awaits([
            call(mock_awaitable1, group_name=grp_name, end_callback=end_cb, cancel_callback=cancel_cb),
            call(mock_coroutine_to_close, group_name=grp_name, end_callback=end_cb, cancel_callback=cancel_cb),
        ])
        mock_coroutine_to_close.close.assert_called_once_with()

    @patch.object(pool, 'create_task')
    @patch.object(pool.TaskPool, '_apply_spawner', new_callable=MagicMock())
    @patch.object(pool, 'TaskGroupRegister')
    @patch.object(pool.TaskPool, '_generate_group_name')
    @patch.object(pool.BaseTaskPool, '_check_start')
    def test_apply(self, mock__check_start: MagicMock, mock__generate_group_name: MagicMock,
                   mock_reg_cls: MagicMock, mock__apply_spawner: MagicMock, mock_create_task: MagicMock):
        mock__generate_group_name.return_value = generated_name = 'name 123'
        mock_group_reg = set_up_mock_group_register(mock_reg_cls)
        mock__apply_spawner.return_value = mock_apply_coroutine = object()
        mock_create_task.return_value = fake_task = object()
        mock_func, num, group_name = MagicMock(), 3, FOO + BAR
        args, kwargs = (FOO, BAR), {'a': 1, 'b': 2}
        end_cb, cancel_cb = MagicMock(), MagicMock()

        self.task_pool._task_groups = {group_name: 'causes error'}
        with self.assertRaises(exceptions.InvalidGroupName):
            self.task_pool.apply(mock_func, args, kwargs, num, group_name, end_cb, cancel_cb)
        mock__check_start.assert_called_once_with(function=mock_func)
        mock__apply_spawner.assert_not_called()
        mock_create_task.assert_not_called()

        mock__check_start.reset_mock()
        self.task_pool._task_groups = {}

        def check_assertions(_group_name, _output):
            self.assertEqual(_group_name, _output)
            mock__check_start.assert_called_once_with(function=mock_func)
            self.assertEqual(mock_group_reg, self.task_pool._task_groups[_group_name])
            mock__apply_spawner.assert_called_once_with(_group_name, mock_func, args, kwargs, num,
                                                        end_callback=end_cb, cancel_callback=cancel_cb)
            mock_create_task.assert_called_once_with(mock_apply_coroutine)
            self.assertSetEqual({fake_task}, self.task_pool._group_meta_tasks_running[group_name])

        output = self.task_pool.apply(mock_func, args, kwargs, num, group_name, end_cb, cancel_cb)
        check_assertions(group_name, output)
        mock__generate_group_name.assert_not_called()

        mock__check_start.reset_mock()
        self.task_pool._task_groups.clear()
        mock__apply_spawner.reset_mock()
        mock_create_task.reset_mock()

        output = self.task_pool.apply(mock_func, args, kwargs, num, None, end_cb, cancel_cb)
        check_assertions(generated_name, output)
        mock__generate_group_name.assert_called_once_with('apply', mock_func)

    @patch.object(pool, 'execute_optional')
    async def test__get_map_end_callback(self, mock_execute_optional: AsyncMock):
        semaphore, mock_end_cb = Semaphore(1), MagicMock()
        wrapped = pool.TaskPool._get_map_end_callback(semaphore, mock_end_cb)
        task_id = 1234
        await wrapped(task_id)
        self.assertEqual(2, semaphore._value)
        mock_execute_optional.assert_awaited_once_with(mock_end_cb, args=(task_id,))

    @patch.object(pool, 'star_function')
    @patch.object(pool.TaskPool, '_start_task')
    @patch.object(pool.TaskPool, '_get_map_end_callback')
    @patch.object(pool, 'Semaphore')
    async def test__queue_consumer(self, mock_semaphore_cls: MagicMock, mock__get_map_end_callback: MagicMock,
                                   mock__start_task: AsyncMock, mock_star_function: MagicMock):
        n = 2
        mock_semaphore_cls.return_value = semaphore = Semaphore(n)
        mock__get_map_end_callback.return_value = map_cb = MagicMock()
        awaitable1, awaitable2 = 'totally an awaitable', object()
        mock_star_function.side_effect = [awaitable1, Exception(), awaitable2]
        arg1, arg2, bad = 123456789, 'function argument', None
        args = [arg1, bad, arg2]
        grp_name, mock_func, stars = 'whatever', MagicMock(__name__="mock"), 3
        end_cb, cancel_cb = MagicMock(), MagicMock()
        self.assertIsNone(await self.task_pool._arg_consumer(grp_name, n, mock_func, args, stars, end_cb, cancel_cb))
        # We initialized the semaphore with a value of 2. It should have been acquired twice. We expect it be locked.
        self.assertTrue(semaphore.locked())
        mock_semaphore_cls.assert_called_once_with(n)
        mock__get_map_end_callback.assert_called_once_with(semaphore, actual_end_callback=end_cb)
        mock__start_task.assert_has_awaits([
            call(awaitable1, group_name=grp_name, ignore_lock=True, end_callback=map_cb, cancel_callback=cancel_cb),
            call(awaitable2, group_name=grp_name, ignore_lock=True, end_callback=map_cb, cancel_callback=cancel_cb),
        ])
        mock_star_function.assert_has_calls([
            call(mock_func, arg1, arg_stars=stars),
            call(mock_func, bad, arg_stars=stars),
            call(mock_func, arg2, arg_stars=stars)
        ])

        mock_semaphore_cls.reset_mock()
        mock__get_map_end_callback.reset_mock()
        mock__start_task.reset_mock()
        mock_star_function.reset_mock(side_effect=True)

        # With a CancelledError thrown while acquiring the semaphore:
        mock_acquire = AsyncMock(side_effect=[True, CancelledError])
        mock_semaphore_cls.return_value = mock_semaphore = MagicMock(acquire=mock_acquire)
        mock_star_function.return_value = mock_coroutine = MagicMock()
        arg_it = iter(arg for arg in (arg1, arg2, FOO))
        self.assertIsNone(await self.task_pool._arg_consumer(grp_name, n, mock_func, arg_it, stars, end_cb, cancel_cb))
        mock_semaphore_cls.assert_called_once_with(n)
        mock__get_map_end_callback.assert_called_once_with(mock_semaphore, actual_end_callback=end_cb)
        mock_star_function.assert_has_calls([
            call(mock_func, arg1, arg_stars=stars),
            call(mock_func, arg2, arg_stars=stars)
        ])
        mock_acquire.assert_has_awaits([call(), call()])
        mock__start_task.assert_awaited_once_with(mock_coroutine, group_name=grp_name, ignore_lock=True,
                                                  end_callback=map_cb, cancel_callback=cancel_cb)
        mock_coroutine.close.assert_called_once_with()
        mock_semaphore.release.assert_not_called()
        self.assertEqual(FOO, next(arg_it))

        mock_acquire.reset_mock(side_effect=True)
        mock_semaphore_cls.reset_mock()
        mock__get_map_end_callback.reset_mock()
        mock__start_task.reset_mock()
        mock_star_function.reset_mock(side_effect=True)

        # With a CancelledError thrown while starting the task:
        mock__start_task.side_effect = [None, CancelledError]
        arg_it = iter(arg for arg in (arg1, arg2, FOO))
        self.assertIsNone(await self.task_pool._arg_consumer(grp_name, n, mock_func, arg_it, stars, end_cb, cancel_cb))
        mock_semaphore_cls.assert_called_once_with(n)
        mock__get_map_end_callback.assert_called_once_with(mock_semaphore, actual_end_callback=end_cb)
        mock_star_function.assert_has_calls([
            call(mock_func, arg1, arg_stars=stars),
            call(mock_func, arg2, arg_stars=stars)
        ])
        mock_acquire.assert_has_awaits([call(), call()])
        mock__start_task.assert_has_awaits(2 * [
            call(mock_coroutine, group_name=grp_name, ignore_lock=True, end_callback=map_cb, cancel_callback=cancel_cb)
        ])
        mock_coroutine.close.assert_called_once_with()
        mock_semaphore.release.assert_called_once_with()
        self.assertEqual(FOO, next(arg_it))

    @patch.object(pool, 'create_task')
    @patch.object(pool.TaskPool, '_arg_consumer', new_callable=MagicMock)
    @patch.object(pool, 'TaskGroupRegister')
    @patch.object(pool.BaseTaskPool, '_check_start')
    def test__map(self, mock__check_start: MagicMock, mock_reg_cls: MagicMock, mock__arg_consumer: MagicMock,
                  mock_create_task: MagicMock):
        mock_group_reg = set_up_mock_group_register(mock_reg_cls)
        mock__arg_consumer.return_value = fake_consumer = object()
        mock_create_task.return_value = fake_task = object()

        group_name, n = 'onetwothree', 0
        func, arg_iter, stars = AsyncMock(), [55, 66, 77], 3
        end_cb, cancel_cb = MagicMock(), MagicMock()

        with self.assertRaises(ValueError):
            self.task_pool._map(group_name, n, func, arg_iter, stars, end_cb, cancel_cb)
        mock__check_start.assert_called_once_with(function=func)

        mock__check_start.reset_mock()

        n = 1234
        self.task_pool._task_groups = {group_name: MagicMock()}

        with self.assertRaises(exceptions.InvalidGroupName):
            self.task_pool._map(group_name, n, func, arg_iter, stars, end_cb, cancel_cb)
        mock__check_start.assert_called_once_with(function=func)

        mock__check_start.reset_mock()

        self.task_pool._task_groups.clear()

        self.assertIsNone(self.task_pool._map(group_name, n, func, arg_iter, stars, end_cb, cancel_cb))
        mock__check_start.assert_called_once_with(function=func)
        mock_reg_cls.assert_called_once_with()
        self.task_pool._task_groups[group_name] = mock_group_reg
        mock__arg_consumer.assert_called_once_with(group_name, n, func, arg_iter, stars,
                                                   end_callback=end_cb, cancel_callback=cancel_cb)
        mock_create_task.assert_called_once_with(fake_consumer)
        self.assertSetEqual({fake_task}, self.task_pool._group_meta_tasks_running[group_name])

    @patch.object(pool.TaskPool, '_map')
    @patch.object(pool.TaskPool, '_generate_group_name')
    def test_map(self, mock__generate_group_name: MagicMock, mock__map: MagicMock):
        mock__generate_group_name.return_value = generated_name = 'name 1 2 3'
        mock_func = MagicMock()
        arg_iter, num_concurrent, group_name = (FOO, BAR, 1, 2, 3), 2, FOO + BAR
        end_cb, cancel_cb = MagicMock(), MagicMock()
        output = self.task_pool.map(mock_func, arg_iter, num_concurrent, group_name, end_cb, cancel_cb)
        self.assertEqual(group_name, output)
        mock__map.assert_called_once_with(group_name, num_concurrent, mock_func, arg_iter, 0,
                                          end_callback=end_cb, cancel_callback=cancel_cb)
        mock__generate_group_name.assert_not_called()

        mock__map.reset_mock()
        output = self.task_pool.map(mock_func, arg_iter, num_concurrent, None, end_cb, cancel_cb)
        self.assertEqual(generated_name, output)
        mock__map.assert_called_once_with(generated_name, num_concurrent, mock_func, arg_iter, 0,
                                          end_callback=end_cb, cancel_callback=cancel_cb)
        mock__generate_group_name.assert_called_once_with('map', mock_func)

    @patch.object(pool.TaskPool, '_map')
    @patch.object(pool.TaskPool, '_generate_group_name')
    def test_starmap(self, mock__generate_group_name: MagicMock, mock__map: MagicMock):
        mock__generate_group_name.return_value = generated_name = 'name 1 2 3'
        mock_func = MagicMock()
        args_iter, num_concurrent, group_name = ([FOO], [BAR]), 2, FOO + BAR
        end_cb, cancel_cb = MagicMock(), MagicMock()
        output = self.task_pool.starmap(mock_func, args_iter, num_concurrent, group_name, end_cb, cancel_cb)
        self.assertEqual(group_name, output)
        mock__map.assert_called_once_with(group_name, num_concurrent, mock_func, args_iter, 1,
                                          end_callback=end_cb, cancel_callback=cancel_cb)
        mock__generate_group_name.assert_not_called()

        mock__map.reset_mock()
        output = self.task_pool.starmap(mock_func, args_iter, num_concurrent, None, end_cb, cancel_cb)
        self.assertEqual(generated_name, output)
        mock__map.assert_called_once_with(generated_name, num_concurrent, mock_func, args_iter, 1,
                                          end_callback=end_cb, cancel_callback=cancel_cb)
        mock__generate_group_name.assert_called_once_with('starmap', mock_func)

    @patch.object(pool.TaskPool, '_map')
    @patch.object(pool.TaskPool, '_generate_group_name')
    async def test_doublestarmap(self, mock__generate_group_name: MagicMock, mock__map: MagicMock):
        mock__generate_group_name.return_value = generated_name = 'name 1 2 3'
        mock_func = MagicMock()
        kw_iter, num_concurrent, group_name = [{'a': FOO}, {'a': BAR}], 2, FOO + BAR
        end_cb, cancel_cb = MagicMock(), MagicMock()
        output = self.task_pool.doublestarmap(mock_func, kw_iter, num_concurrent, group_name, end_cb, cancel_cb)
        self.assertEqual(group_name, output)
        mock__map.assert_called_once_with(group_name, num_concurrent, mock_func, kw_iter, 2,
                                          end_callback=end_cb, cancel_callback=cancel_cb)
        mock__generate_group_name.assert_not_called()

        mock__map.reset_mock()
        output = self.task_pool.doublestarmap(mock_func, kw_iter, num_concurrent, None, end_cb, cancel_cb)
        self.assertEqual(generated_name, output)
        mock__map.assert_called_once_with(generated_name, num_concurrent, mock_func, kw_iter, 2,
                                          end_callback=end_cb, cancel_callback=cancel_cb)
        mock__generate_group_name.assert_called_once_with('doublestarmap', mock_func)


class SimpleTaskPoolTestCase(CommonTestCase):
    TEST_CLASS = pool.SimpleTaskPool
    task_pool: pool.SimpleTaskPool

    TEST_POOL_FUNC = AsyncMock(__name__=FOO)
    TEST_POOL_ARGS = (FOO, BAR)
    TEST_POOL_KWARGS = {'a': 1, 'b': 2}
    TEST_POOL_END_CB = MagicMock()
    TEST_POOL_CANCEL_CB = MagicMock()

    def get_task_pool_init_params(self) -> dict:
        params = super().get_task_pool_init_params()
        params.update({
            'func': self.TEST_POOL_FUNC,
            'args': self.TEST_POOL_ARGS,
            'kwargs': self.TEST_POOL_KWARGS,
            'end_callback': self.TEST_POOL_END_CB,
            'cancel_callback': self.TEST_POOL_CANCEL_CB,
        })
        return params

    def setUp(self) -> None:
        self.base_class_init_patcher = patch.object(pool.BaseTaskPool, '__init__')
        self.base_class_init = self.base_class_init_patcher.start()
        super().setUp()

    def tearDown(self) -> None:
        self.base_class_init_patcher.stop()
        super().tearDown()

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
    async def test__start_num(self, mock__start_task: AsyncMock):
        group_name = FOO + BAR + 'abc'
        mock_awaitable1, mock_awaitable2 = object(), object()
        self.task_pool._func = MagicMock(side_effect=[mock_awaitable1, Exception(), mock_awaitable2], __name__='func')
        num = 3
        self.assertIsNone(await self.task_pool._start_num(num, group_name))
        self.task_pool._func.assert_has_calls(num * [call(*self.task_pool._args, **self.task_pool._kwargs)])
        call_kw = {
            'group_name': group_name,
            'end_callback': self.task_pool._end_callback,
            'cancel_callback': self.task_pool._cancel_callback
        }
        mock__start_task.assert_has_awaits([call(mock_awaitable1, **call_kw), call(mock_awaitable2, **call_kw)])

        self.task_pool._func.reset_mock(side_effect=True)
        mock__start_task.reset_mock()

        # Simulate cancellation while the second task is being started.
        mock__start_task.side_effect = [None, CancelledError, None]
        mock_coroutine_to_close = MagicMock()
        self.task_pool._func.side_effect = [mock_awaitable1, mock_coroutine_to_close, 'never called']
        self.assertIsNone(await self.task_pool._start_num(num, group_name))
        self.task_pool._func.assert_has_calls(2 * [call(*self.task_pool._args, **self.task_pool._kwargs)])
        mock__start_task.assert_has_awaits([call(mock_awaitable1, **call_kw), call(mock_coroutine_to_close, **call_kw)])
        mock_coroutine_to_close.close.assert_called_once_with()

    @patch.object(pool, 'create_task')
    @patch.object(pool.SimpleTaskPool, '_start_num', new_callable=MagicMock())
    @patch.object(pool, 'TaskGroupRegister')
    @patch.object(pool.BaseTaskPool, '_check_start')
    def test_start(self, mock__check_start: MagicMock, mock_reg_cls: MagicMock, mock__start_num: AsyncMock,
                   mock_create_task: MagicMock):
        mock_group_reg = set_up_mock_group_register(mock_reg_cls)
        mock__start_num.return_value = mock_start_num_coroutine = object()
        mock_create_task.return_value = fake_task = object()
        self.task_pool._task_groups = {}
        self.task_pool._group_meta_tasks_running = {}
        num = 5
        self.task_pool._start_calls = 42
        expected_group_name = 'start-group-42'
        output = self.task_pool.start(num)
        self.assertEqual(expected_group_name, output)
        mock__check_start.assert_called_once_with(function=self.TEST_POOL_FUNC)
        self.assertEqual(43, self.task_pool._start_calls)
        self.assertEqual(mock_group_reg, self.task_pool._task_groups[expected_group_name])
        mock__start_num.assert_called_once_with(num, expected_group_name)
        mock_create_task.assert_called_once_with(mock_start_num_coroutine)
        self.assertSetEqual({fake_task}, self.task_pool._group_meta_tasks_running[expected_group_name])

    @patch.object(pool.SimpleTaskPool, 'cancel')
    def test_stop(self, mock_cancel: MagicMock):
        num = 2
        id1, id2, id3 = 5, 6, 7
        self.task_pool._tasks_running = {id1: FOO, id2: BAR, id3: FOO + BAR}
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


def set_up_mock_group_register(mock_reg_cls: MagicMock) -> MagicMock:
    mock_grp_aenter, mock_grp_aexit, mock_grp_add = AsyncMock(), AsyncMock(), MagicMock()
    mock_reg_cls.return_value = mock_group_reg = MagicMock(__aenter__=mock_grp_aenter, __aexit__=mock_grp_aexit,
                                                           add=mock_grp_add)
    return mock_group_reg
