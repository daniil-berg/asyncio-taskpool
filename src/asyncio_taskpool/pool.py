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
This module contains the definitions of the task pool classes.

A task pool is an object with a simple interface for aggregating and dynamically managing asynchronous tasks.
Generally speaking, a task is added to a pool by providing it with a coroutine function reference as well as the 
arguments for that function.

The `BaseTaskPool` class is a parent class and not intended for direct use.
The `TaskPool` and `SimpleTaskPool` are subclasses intended for direct use.
While the former allows for heterogeneous collections of tasks that can be entirely unrelated to one another, the 
latter requires a preemptive decision about the function **and** its arguments upon initialization and only allows
to dynamically control the **number** of tasks running at any point in time.

For further details about the classes check their respective docstrings.
"""


import logging
from asyncio.coroutines import iscoroutine, iscoroutinefunction
from asyncio.exceptions import CancelledError
from asyncio.locks import Semaphore
from asyncio.queues import QueueEmpty
from asyncio.tasks import Task, create_task, gather
from contextlib import suppress
from datetime import datetime
from math import inf
from typing import Any, Awaitable, Dict, Iterable, Iterator, List, Set

from . import exceptions
from .constants import DEFAULT_TASK_GROUP, DATETIME_FORMAT
from .group_register import TaskGroupRegister
from .helpers import execute_optional, star_function, join_queue
from .queue_context import Queue
from .types import ArgsT, KwArgsT, CoroutineFunc, EndCB, CancelCB


log = logging.getLogger(__name__)


class BaseTaskPool:
    """The base class for task pools. Not intended to be used directly."""
    _pools: List['BaseTaskPool'] = []

    @classmethod
    def _add_pool(cls, pool: 'BaseTaskPool') -> int:
        """Adds a `pool` (instance of any subclass) to the general list of pools and returns it's index in the list."""
        cls._pools.append(pool)
        return len(cls._pools) - 1

    def __init__(self, pool_size: int = inf, name: str = None) -> None:
        """Initializes the necessary internal attributes and adds the new pool to the general pools list."""
        # Initialize a counter for the total number of tasks started through the pool and one for the total number of
        # tasks cancelled through the pool.
        self._num_started: int = 0
        self._num_cancellations: int = 0

        # Initialize flags; immutably set the name.
        self._locked: bool = False
        self._closed: bool = False
        self._name: str = name

        # The following three dictionaries are the actual containers of the tasks controlled by the pool.
        self._tasks_running: Dict[int, Task] = {}
        self._tasks_cancelled: Dict[int, Task] = {}
        self._tasks_ended: Dict[int, Task] = {}

        # These next three attributes act as synchronisation primitives necessary for managing the pool.
        self._before_gathering: List[Awaitable] = []
        self._enough_room: Semaphore = Semaphore()
        self._task_groups: Dict[str, TaskGroupRegister[int]] = {}

        # Finish with method/functions calls that add the pool to the internal list of pools, set its initial size,
        # and issue a log message.
        self._idx: int = self._add_pool(self)
        self.pool_size = pool_size
        log.debug("%s initialized", str(self))

    def __str__(self) -> str:
        """Returns the name of the task pool."""
        return f'{self.__class__.__name__}-{self._name or self._idx}'

    @property
    def pool_size(self) -> int:
        """Returns the maximum number of concurrently running tasks currently set in the pool."""
        return self._pool_size

    @pool_size.setter
    def pool_size(self, value: int) -> None:
        """
        Sets the maximum number of concurrently running tasks in the pool.

        Args:
            value:
                A non-negative integer.
                NOTE: Increasing the pool size will immediately start tasks that are awaiting enough room to run.

        Raises:
            `ValueError` if `value` is less than 0.
        """
        if value < 0:
            raise ValueError("Pool size can not be less than 0")
        self._enough_room._value = value
        self._pool_size = value

    @property
    def is_locked(self) -> bool:
        """Returns `True` if more the pool has been locked (see below)."""
        return self._locked

    def lock(self) -> None:
        """Disallows any more tasks to be started in the pool."""
        if not self._locked:
            self._locked = True
            log.info("%s is locked!", str(self))

    def unlock(self) -> None:
        """Allows new tasks to be started in the pool."""
        if self._locked:
            self._locked = False
            log.info("%s was unlocked.", str(self))

    @property
    def num_running(self) -> int:
        """
        Returns the number of tasks in the pool that are (at that moment) still running.

        At the moment a task's `end_callback` or `cancel_callback` is fired, it is no longer considered running.
        """
        return len(self._tasks_running)

    @property
    def num_cancellations(self) -> int:
        """
        Returns the number of tasks in the pool that have been cancelled through the pool (up until that moment).

        At the moment a task's `cancel_callback` is fired, this counts as a cancellation, and the task is then
        considered cancelled (instead of running) until its `end_callback` is fired.
        """
        return self._num_cancellations

    @property
    def num_ended(self) -> int:
        """
        Returns the number of tasks started through the pool that have stopped running (up until that moment).

        At the moment a task's `end_callback` is fired, it is considered ended and no longer running (or cancelled).
        When a task is cancelled, it is not immediately considered ended; only after its `cancel_callback` has returned,
        does it then actually end.
        """
        return len(self._tasks_ended)

    @property
    def num_finished(self) -> int:
        """Returns the number of tasks in the pool that have finished running (without having been cancelled)."""
        return len(self._tasks_ended) - self._num_cancellations + len(self._tasks_cancelled)

    @property
    def is_full(self) -> bool:
        """
        Returns `False` only if (at that moment) the number of running tasks is below the pool's specified size.
        When the pool is full, any call to start a new task within it will block.
        """
        return self._enough_room.locked()

    def get_task_group_ids(self, group_name: str) -> Set[int]:
        """
        Returns the set of IDs of all tasks in the specified group.

        Args:
            group_name: Must be a name of a task group that exists within the pool.

        Returns:
            Set of integers representing the task IDs belonging to the specified group.

        Raises:
            `InvalidGroupName` if no task group named `group_name` exists in the pool.
        """
        try:
            return set(self._task_groups[group_name])
        except KeyError:
            raise exceptions.InvalidGroupName(f"No task group named {group_name} exists in this pool.")

    def _check_start(self, *, awaitable: Awaitable = None, function: CoroutineFunc = None,
                     ignore_lock: bool = False) -> None:
        """
        Checks necessary conditions for starting a task (group) in the pool.

        Either something that is expected to be a coroutine (i.e. awaitable) or something that is expected to be a
        coroutine _function_ will be checked.

        Args:
            awaitable: If this is passed, `function` must be `None`.
            function: If this is passed, `awaitable` must be `None`.
            ignore_lock (optional): If `True`, a locked pool will produce no error here.

        Raises:
            `AssertionError` if both or neither of `awaitable` and `function` were passed.
            `asyncio_taskpool.exceptions.PoolIsClosed` if the pool is closed.
            `asyncio_taskpool.exceptions.NotCoroutine` if `awaitable` is not a cor. / `function` not a cor. func.
            `asyncio_taskpool.exceptions.PoolIsLocked` if the pool has been locked and `ignore_lock` is `False`.
        """
        assert (awaitable is None) != (function is None)
        if awaitable and not iscoroutine(awaitable):
            raise exceptions.NotCoroutine(f"Not awaitable: {awaitable}")
        if function and not iscoroutinefunction(function):
            raise exceptions.NotCoroutine(f"Not a coroutine function: {function}")
        if self._closed:
            raise exceptions.PoolIsClosed("You must use another pool")
        if self._locked and not ignore_lock:
            raise exceptions.PoolIsLocked("Cannot start new tasks")

    def _task_name(self, task_id: int) -> str:
        """Returns a standardized name for a task with a specific `task_id`."""
        return f'{self}_Task-{task_id}'

    async def _task_cancellation(self, task_id: int, custom_callback: CancelCB = None) -> None:
        """
        Universal callback to be run upon any task in the pool being cancelled.

        Required for keeping track of running/cancelled tasks and proper logging.

        Args:
            task_id:
                The ID of the task that has been cancelled.
            custom_callback (optional):
                A callback to execute after cancellation of the task.
                It is run at the end of this function with the `task_id` as its only positional argument.
        """
        log.debug("Cancelling %s ...", self._task_name(task_id))
        self._tasks_cancelled[task_id] = self._tasks_running.pop(task_id)
        self._num_cancellations += 1
        log.debug("Cancelled %s", self._task_name(task_id))
        await execute_optional(custom_callback, args=(task_id,))

    async def _task_ending(self, task_id: int, custom_callback: EndCB = None) -> None:
        """
        Universal callback to be run upon any task in the pool ending its work.

        Required for keeping track of running/cancelled/ended tasks and proper logging.
        Also releases room in the task pool for potentially waiting tasks.

        Args:
            task_id:
                The ID of the task that has reached its end.
            custom_callback (optional):
                A callback to execute after the task has ended.
                It is run at the end of this function with the `task_id` as its only positional argument.
        """
        try:
            self._tasks_ended[task_id] = self._tasks_running.pop(task_id)
        except KeyError:
            self._tasks_ended[task_id] = self._tasks_cancelled.pop(task_id)
        self._enough_room.release()
        log.info("Ended %s", self._task_name(task_id))
        await execute_optional(custom_callback, args=(task_id,))

    async def _task_wrapper(self, awaitable: Awaitable, task_id: int, end_callback: EndCB = None,
                            cancel_callback: CancelCB = None) -> Any:
        """
        Universal wrapper around every task run in the pool that returns/raises whatever the wrapped coroutine does.

        Responsible for catching cancellation and awaiting the `_task_cancellation` callback, as well as for awaiting
        the `_task_ending` callback, after the coroutine returns or raises an exception.

        Args:
            awaitable:
                The actual coroutine to be run within the task pool.
            task_id:
                The ID of the newly created task.
            end_callback (optional):
                A callback to execute after the task has ended.
                It is run with the `task_id` as its only positional argument.
            cancel_callback (optional):
                A callback to execute after cancellation of the task.
                It is run with the `task_id` as its only positional argument.
        """
        log.info("Started %s", self._task_name(task_id))
        try:
            return await awaitable
        except CancelledError:
            await self._task_cancellation(task_id, custom_callback=cancel_callback)
        finally:
            await self._task_ending(task_id, custom_callback=end_callback)

    async def _start_task(self, awaitable: Awaitable, group_name: str = DEFAULT_TASK_GROUP, ignore_lock: bool = False,
                          end_callback: EndCB = None, cancel_callback: CancelCB = None) -> int:
        """
        Starts a coroutine as a new task in the pool.

        This method can block for a significant amount of time, **only if** the pool is full.
        Otherwise it merely needs to acquire the `TaskGroupRegister` lock, which should never be held for a long time.

        Args:
            awaitable:
                The actual coroutine to be run within the task pool.
            group_name (optional):
                Name of the task group to add the new task to; defaults to the `DEFAULT_TASK_GROUP` constant.
            ignore_lock (optional):
                If `True`, even if the pool is locked, the task will still be started.
            end_callback (optional):
                A callback to execute after the task has ended.
                It is run with the task's ID as its only positional argument.
            cancel_callback (optional):
                A callback to execute after cancellation of the task.
                It is run with the task's ID as its only positional argument.

        Returns:
            The ID of the newly started task.
        """
        self._check_start(awaitable=awaitable, ignore_lock=ignore_lock)
        await self._enough_room.acquire()
        group_reg = self._task_groups.setdefault(group_name, TaskGroupRegister())
        async with group_reg:
            task_id = self._num_started
            self._num_started += 1
            group_reg.add(task_id)
            self._tasks_running[task_id] = create_task(
                coro=self._task_wrapper(awaitable, task_id, end_callback, cancel_callback),
                name=self._task_name(task_id)
            )
        return task_id

    def _get_running_task(self, task_id: int) -> Task:
        """
        Gets a running task by its task ID.

        Args:
            task_id: The ID of a task still running within the pool.

        Raises:
            `asyncio_taskpool.exceptions.AlreadyCancelled` if the task with `task_id` has been (recently) cancelled.
            `asyncio_taskpool.exceptions.AlreadyEnded` if the task with `task_id` has ended (recently).
            `asyncio_taskpool.exceptions.InvalidTaskID` if no task with `task_id` is known to the pool.
        """
        try:
            return self._tasks_running[task_id]
        except KeyError:
            if self._tasks_cancelled.get(task_id):
                raise exceptions.AlreadyCancelled(f"{self._task_name(task_id)} has been cancelled")
            if self._tasks_ended.get(task_id):
                raise exceptions.AlreadyEnded(f"{self._task_name(task_id)} has finished running")
            raise exceptions.InvalidTaskID(f"No task with ID {task_id} found in {self}")

    def cancel(self, *task_ids: int, msg: str = None) -> None:
        """
        Cancels the tasks with the specified IDs.

        Each task ID must belong to a task still running within the pool. Otherwise one of the following exceptions will
        be raised:
        - `AlreadyCancelled` if one of the `task_ids` belongs to a task that has been (recently) cancelled.
        - `AlreadyEnded` if one of the `task_ids` belongs to a task that has ended (recently).
        - `InvalidTaskID` if any of the `task_ids` is not known to the pool.
        Note that once a pool has been flushed (see below), IDs of tasks that have ended previously will be forgotten.

        Args:
            task_ids: Arbitrary number of integers. Each must be an ID of a task still running within the pool.
            msg (optional): Passed to the `Task.cancel()` method of every task specified by the `task_ids`.
        """
        tasks = [self._get_running_task(task_id) for task_id in task_ids]
        for task in tasks:
            task.cancel(msg=msg)

    def _cancel_and_remove_all_from_group(self, group_name: str, group_reg: TaskGroupRegister, msg: str = None) -> None:
        """
        Removes all tasks from the specified group and cancels them, if they are still running.

        Args:
            group_name: The name of the group of tasks that shall be cancelled.
            group_reg: The task group register object containing the task IDs.
            msg (optional): Passed to the `Task.cancel()` method of every task specified by the `task_ids`.
        """
        while group_reg:
            try:
                self._tasks_running[group_reg.pop()].cancel(msg=msg)
            except KeyError:
                continue
        log.debug("%s cancelled tasks from group %s", str(self), group_name)

    async def cancel_group(self, group_name: str, msg: str = None) -> None:
        """
        Cancels an entire group of tasks. The task group is subsequently forgotten by the pool.

        Args:
            group_name: The name of the group of tasks that shall be cancelled.
            msg (optional): Passed to the `Task.cancel()` method of every task specified by the `task_ids`.

        Raises:
            `InvalidGroupName` if no task group named `group_name` exists in the pool.
        """
        log.debug("%s cancelling tasks in group %s", str(self), group_name)
        try:
            group_reg = self._task_groups.pop(group_name)
        except KeyError:
            raise exceptions.InvalidGroupName(f"No task group named {group_name} exists in this pool.")
        async with group_reg:
            self._cancel_and_remove_all_from_group(group_name, group_reg, msg=msg)
        log.debug("%s forgot task group %s", str(self), group_name)

    async def cancel_all(self, msg: str = None) -> None:
        """
        Cancels all tasks still running within the pool.

        Args:
            msg (optional): Passed to the `Task.cancel()` method of every task specified by the `task_ids`.
        """
        log.warning("%s cancelling all tasks!", str(self))
        while self._task_groups:
            group_name, group_reg = self._task_groups.popitem()
            async with group_reg:
                self._cancel_and_remove_all_from_group(group_name, group_reg, msg=msg)

    async def flush(self, return_exceptions: bool = False):
        """
        Calls `asyncio.gather` on all ended/cancelled tasks from the pool, and forgets the tasks.

        This method exists mainly to free up memory of unneeded `Task` objects.

        This method blocks, **only if** any of the tasks block while catching a `asyncio.CancelledError` or any of the
        callbacks registered for the tasks block.

        Args:
            return_exceptions (optional): Passed directly into `gather`.
        """
        await gather(*self._tasks_ended.values(), *self._tasks_cancelled.values(), return_exceptions=return_exceptions)
        self._tasks_ended.clear()
        self._tasks_cancelled.clear()

    async def gather_and_close(self, return_exceptions: bool = False):
        """
        Calls `asyncio.gather` on **all** tasks in the pool, then permanently closes the pool.

        The `lock()` method must have been called prior to this.

        This method may block, if one of the tasks blocks while catching a `asyncio.CancelledError` or if any of the
        callbacks registered for a task blocks for whatever reason.

        Args:
            return_exceptions (optional): Passed directly into `gather`.

        Raises:
            `PoolStillUnlocked` if the pool has not been locked yet.
        """
        if not self._locked:
            raise exceptions.PoolStillUnlocked("Pool must be locked, before tasks can be gathered")
        await gather(*self._before_gathering)
        await gather(*self._tasks_ended.values(), *self._tasks_cancelled.values(), *self._tasks_running.values(),
                     return_exceptions=return_exceptions)
        self._tasks_ended.clear()
        self._tasks_cancelled.clear()
        self._tasks_running.clear()
        self._before_gathering.clear()
        self._closed = True


class TaskPool(BaseTaskPool):
    """
    General task pool class. Attempts to emulate part of the interface of `multiprocessing.pool.Pool` from the stdlib.

    A `TaskPool` instance can manage an arbitrary number of concurrent tasks from any coroutine function.
    Tasks in the pool can all belong to the same coroutine function,
    but they can also come from any number of different and unrelated coroutine functions.

    As long as there is room in the pool, more tasks can be added. (By default, there is no pool size limit.)
    Each task started in the pool receives a unique ID, which can be used to cancel specific tasks at any moment.

    Adding tasks blocks **only if** the pool is full at that moment.
    """

    _QUEUE_END_SENTINEL = object()

    def __init__(self, pool_size: int = inf, name: str = None) -> None:
        super().__init__(pool_size=pool_size, name=name)
        # In addition to all the attributes of the base class, we need a dictionary mapping task group names to sets of
        # meta tasks that are/were running in the context of that group, and a bucked for cancelled meta tasks.
        self._group_meta_tasks_running: Dict[str, Set[Task]] = {}
        self._meta_tasks_cancelled: Set[Task] = set()

    def _cancel_group_meta_tasks(self, group_name: str) -> None:
        """Cancels and forgets all meta tasks associated with the task group named `group_name`."""
        try:
            meta_tasks = self._group_meta_tasks_running.pop(group_name)
        except KeyError:
            return
        for meta_task in meta_tasks:
            meta_task.cancel()
        self._meta_tasks_cancelled.update(meta_tasks)
        log.debug("%s cancelled and forgot meta tasks from group %s", str(self), group_name)

    def _cancel_and_remove_all_from_group(self, group_name: str, group_reg: TaskGroupRegister, msg: str = None) -> None:
        self._cancel_group_meta_tasks(group_name)
        super()._cancel_and_remove_all_from_group(group_name, group_reg, msg=msg)

    async def cancel_group(self, group_name: str, msg: str = None) -> None:
        """
        Cancels an entire group of tasks. The task group is subsequently forgotten by the pool.

        If any methods such as `map()` launched meta tasks belonging to that group, these meta tasks are cancelled
        before the actual tasks are cancelled. This means that any tasks "queued" to be started by a meta task will
        **never even start**. In the case of `map()` this would mean that the `arg_iter` may be abandoned before it
        was fully consumed (if that is even possible).

        Args:
            group_name: The name of the group of tasks (and meta tasks) that shall be cancelled.
            msg (optional): Passed to the `Task.cancel()` method of every task specified by the `task_ids`.

        Raises:
            `InvalidGroupName` if no task group named `group_name` exists in the pool.
        """
        await super().cancel_group(group_name=group_name, msg=msg)

    async def cancel_all(self, msg: str = None) -> None:
        """
        Cancels all tasks still running within the pool. (This includes all meta tasks.)

        If any methods such as `map()` launched meta tasks, these meta tasks are cancelled before the actual tasks are
        cancelled. This means that any tasks "queued" to be started by a meta task will **never even start**. In the
        case of `map()` this would mean that the `arg_iter` may be abandoned before it was fully consumed (if that is
        even possible).

        Args:
            msg (optional): Passed to the `Task.cancel()` method of every task specified by the `task_ids`.
        """
        await super().cancel_all(msg=msg)

    def _pop_ended_meta_tasks(self) -> Set[Task]:
        """
        Goes through all not-cancelled meta tasks, checks if they are done already, and returns those that are.

        The internal `_group_meta_tasks_running` dictionary is updated accordingly, i.e. after this method returns, the
        values (sets) only contain meta tasks that were not done yet. In addition, names of groups that no longer
        have any running meta tasks associated with them are removed from the keys.
        """
        obsolete_keys, ended_meta_tasks = [], set()
        for group_name in self._group_meta_tasks_running.keys():
            still_running = set()
            while self._group_meta_tasks_running[group_name]:
                meta_task = self._group_meta_tasks_running[group_name].pop()
                if meta_task.done():
                    ended_meta_tasks.add(meta_task)
                else:
                    still_running.add(meta_task)
            if still_running:
                self._group_meta_tasks_running[group_name] = still_running
            else:
                obsolete_keys.append(group_name)
        # If a group no longer has running meta tasks associated with, we can remove its name from the dictionary.
        for group_name in obsolete_keys:
            del self._group_meta_tasks_running[group_name]
        return ended_meta_tasks

    async def flush(self, return_exceptions: bool = False):
        """
        Calls `asyncio.gather` on all ended/cancelled tasks from the pool, and forgets the tasks.

        This method exists mainly to free up memory of unneeded `Task` objects. It also gets rid of unneeded meta tasks.

        This method blocks, **only if** any of the tasks block while catching a `asyncio.CancelledError` or any of the
        callbacks registered for the tasks block.

        Args:
            return_exceptions (optional): Passed directly into `gather`.
        """
        await super().flush(return_exceptions=return_exceptions)
        with suppress(CancelledError):
            await gather(*self._meta_tasks_cancelled, *self._pop_ended_meta_tasks(),
                         return_exceptions=return_exceptions)
        self._meta_tasks_cancelled.clear()

    async def gather_and_close(self, return_exceptions: bool = False):
        """
        Calls `asyncio.gather` on **all** tasks in the pool, then permanently closes the pool.

        The `lock()` method must have been called prior to this.

        Note that this method may block indefinitely as long as any task in the pool is not done. This includes meta
        tasks launched my methods such as `map()`, which ends by itself, only once the `arg_iter` is fully consumed,
        which may not even be possible (depending on what the iterable of arguments represents). If you want to avoid
        this, make sure to call `cancel_all()` prior to this.


        This method may also block, if one of the tasks blocks while catching a `asyncio.CancelledError` or if any of
        the callbacks registered for a task blocks for whatever reason.

        Args:
            return_exceptions (optional): Passed directly into `gather`.

        Raises:
            `PoolStillUnlocked` if the pool has not been locked yet.
        """
        await super().gather_and_close(return_exceptions=return_exceptions)
        not_cancelled_meta_tasks = set()
        while self._group_meta_tasks_running:
            _, meta_tasks = self._group_meta_tasks_running.popitem()
            not_cancelled_meta_tasks.update(meta_tasks)
        with suppress(CancelledError):
            await gather(*self._meta_tasks_cancelled, *not_cancelled_meta_tasks, return_exceptions=return_exceptions)
        self._meta_tasks_cancelled.clear()

    @staticmethod
    def _generate_group_name(prefix: str, coroutine_function: CoroutineFunc) -> str:
        """
        Creates a task group identifier that includes the current datetime.

        Args:
            prefix: The start of the name; will be followed by an underscore.
            coroutine_function: The function representing the task group.

        Returns:
            The constructed 'prefix_function_datetime' string to name a task group.
        """
        return f'{prefix}_{coroutine_function.__name__}_{datetime.now().strftime(DATETIME_FORMAT)}'

    async def _apply_num(self, group_name: str, func: CoroutineFunc, args: ArgsT = (), kwargs: KwArgsT = None,
                         num: int = 1, end_callback: EndCB = None, cancel_callback: CancelCB = None) -> None:
        """
        Creates a coroutine with the supplied arguments and runs it as a new task in the pool.

        This method blocks, **only if** the pool has not enough room to accommodate `num` new tasks.

        Args:
            group_name:
                Name of the task group to add the new task to.
            func:
                The coroutine function to be run as a task within the task pool.
            args (optional):
                The positional arguments to pass into the function call.
            kwargs (optional):
                The keyword-arguments to pass into the function call.
            num (optional):
                The number of tasks to spawn with the specified parameters.
            end_callback (optional):
                A callback to execute after the task has ended.
                It is run with the task's ID as its only positional argument.
            cancel_callback (optional):
                A callback to execute after cancellation of the task.
                It is run with the task's ID as its only positional argument.
        """
        if kwargs is None:
            kwargs = {}
        await gather(*(self._start_task(func(*args, **kwargs), group_name=group_name, end_callback=end_callback,
                                        cancel_callback=cancel_callback) for _ in range(num)))

    async def apply(self, func: CoroutineFunc, args: ArgsT = (), kwargs: KwArgsT = None, num: int = 1,
                    group_name: str = None, end_callback: EndCB = None, cancel_callback: CancelCB = None) -> str:
        """
        Creates an arbitrary number of coroutines with the supplied arguments and runs them as new tasks in the pool.

        Each coroutine looks like `func(*args, **kwargs)`. All the new tasks are added to the same task group.
        This method blocks, **only if** the pool has not enough room to accommodate `num` new tasks.

        Args:
            func:
                The coroutine function to use for spawning the new tasks within the task pool.
            args (optional):
                The positional arguments to pass into each function call.
            kwargs (optional):
                The keyword-arguments to pass into each function call.
            num (optional):
                The number of tasks to spawn with the specified parameters.
            group_name (optional):
                Name of the task group to add the new tasks to.
            end_callback (optional):
                A callback to execute after a task has ended.
                It is run with the task's ID as its only positional argument.
            cancel_callback (optional):
                A callback to execute after cancellation of a task.
                It is run with the task's ID as its only positional argument.

        Returns:
            The name of the task group that the newly spawned tasks have been added to.

        Raises:
            `PoolIsClosed` if the pool is closed.
            `NotCoroutine` if `func` is not a coroutine function.
            `PoolIsLocked` if the pool has been locked.
        """
        self._check_start(function=func)
        if group_name is None:
            group_name = self._generate_group_name('apply', func)
        group_reg = self._task_groups.setdefault(group_name, TaskGroupRegister())
        async with group_reg:
            task = create_task(self._apply_num(group_name, func, args, kwargs, num, end_callback, cancel_callback))
        await task
        return group_name

    @classmethod
    async def _queue_producer(cls, arg_queue: Queue, arg_iter: Iterator[Any], group_name: str) -> None:
        """
        Keeps the arguments queue from `_map()` full as long as the iterator has elements.

        Intended to be run as a meta task of a specific group.

        Args:
            arg_queue: The queue of function arguments to consume for starting a new task.
            arg_iter: The iterator of function arguments to put into the queue.
            group_name: Name of the task group associated with this producer.
        """
        try:
            for arg in arg_iter:
                await arg_queue.put(arg)  # This blocks as long as the queue is full.
        except CancelledError:
            # This means that no more tasks are supposed to be created from this `_map()` call;
            # thus, we can immediately drain the entire queue and forget about the rest of the arguments.
            log.debug("Cancelled consumption of argument iterable in task group '%s'", group_name)
            while True:
                try:
                    arg_queue.get_nowait()
                    arg_queue.item_processed()
                except QueueEmpty:
                    return
        finally:
            await arg_queue.put(cls._QUEUE_END_SENTINEL)

    @staticmethod
    def _get_map_end_callback(map_semaphore: Semaphore, actual_end_callback: EndCB) -> EndCB:
        """Returns a wrapped `end_callback` for each `_queue_consumer()` task that will release the `map_semaphore`."""
        async def release_callback(task_id: int) -> None:
            map_semaphore.release()
            await execute_optional(actual_end_callback, args=(task_id,))
        return release_callback

    async def _queue_consumer(self, arg_queue: Queue, group_name: str, func: CoroutineFunc, arg_stars: int = 0,
                              end_callback: EndCB = None, cancel_callback: CancelCB = None) -> None:
        """
        Consumes arguments from the queue from `_map()` and keeps a limited number of tasks working on them.

        The queue's maximum size is taken as the limiting value of an internal semaphore, which must be acquired before
        a new task can be started, and which must be released when one of these tasks ends.

        Intended to be run as a meta task of a specific group.

        Args:
            arg_queue:
                The queue of function arguments to consume for starting a new task.
            group_name:
                Name of the associated task group; passed into the `_start_task()` method.
            func:
                The coroutine function to use for spawning the new tasks within the task pool.
            arg_stars (optional):
                Whether or not to unpack an element from `arg_queue` using stars; must be 0, 1, or 2.
            end_callback (optional):
                The actual callback specified to execute after the task (and the next one) has ended.
                It is run with the task's ID as its only positional argument.
            cancel_callback (optional):
                The callback that was specified to execute after cancellation of the task (and the next one).
                It is run with the task's ID as its only positional argument.
        """
        map_semaphore = Semaphore(arg_queue.maxsize)  # value determined by `group_size` in `_map()`
        release_cb = self._get_map_end_callback(map_semaphore, actual_end_callback=end_callback)
        while True:
            # The following line blocks **only if** the number of running tasks spawned by this method has reached the
            # specified maximum as determined in the `_map()` method.
            await map_semaphore.acquire()
            # We await the queue's `get()` coroutine and subsequently ensure that its `task_done()` method is called.
            async with arg_queue as next_arg:
                if next_arg is self._QUEUE_END_SENTINEL:
                    # The `_queue_producer()` either reached the last argument or was cancelled.
                    return
                await self._start_task(star_function(func, next_arg, arg_stars=arg_stars), group_name=group_name,
                                       ignore_lock=True, end_callback=release_cb, cancel_callback=cancel_callback)

    async def _map(self, group_name: str, group_size: int, func: CoroutineFunc, arg_iter: ArgsT, arg_stars: int,
                   end_callback: EndCB = None, cancel_callback: CancelCB = None) -> None:
        """
        Creates coroutines with arguments from the supplied iterable and runs them as new tasks in the pool.

        Each coroutine looks like `func(arg)`, `func(*arg)`, or `func(**arg)`, `arg` being taken from `arg_iter`.
        All the new tasks are added to the same task group.

        The `group_size` determines the maximum number of tasks spawned this way that shall be running concurrently at
        any given moment in time. As soon as one task from this group ends, it triggers the start of a new task
        (assuming there is room in the pool), which consumes the next element from the arguments iterable. If the size
        of the pool never imposes a limit, this ensures that the number of tasks belonging to this group and running
        concurrently is always equal to `group_size` (except for when `arg_iter` is exhausted of course).

        This method sets up an internal arguments queue which is continuously filled while consuming the `arg_iter`.
        Because this method delegates the spawning of the tasks to two meta tasks (a producer and a consumer of the
        aforementioned queue), it **never blocks**. However, just because this method returns immediately, this does
        not mean that any task was started or that any number of tasks will start soon, as this is solely determined by
        the `pool_size` and the `group_size`.

        Args:
            group_name:
                Name of the task group to add the new tasks to. It must be a name that doesn't exist yet.
            group_size:
                The maximum number new tasks spawned by this method to run concurrently.
            func:
                The coroutine function to use for spawning the new tasks within the task pool.
            arg_iter:
                The iterable of arguments; each element is to be passed into a `func` call when spawning a new task.
            arg_stars:
                Whether or not to unpack an element from `args_iter` using stars; must be 0, 1, or 2.
            end_callback (optional):
                A callback to execute after a task has ended.
                It is run with the task's ID as its only positional argument.
            cancel_callback (optional):
                A callback to execute after cancellation of a task.
                It is run with the task's ID as its only positional argument.

        Raises:
            `ValueError` if `group_size` is less than 1.
            `asyncio_taskpool.exceptions.InvalidGroupName` if a group named `group_name` exists in the pool.
        """
        self._check_start(function=func)
        if group_size < 1:
            raise ValueError(f"Group size must be a positive integer.")
        if group_name in self._task_groups.keys():
            raise exceptions.InvalidGroupName(f"Group named {group_name} already exists!")
        self._task_groups[group_name] = group_reg = TaskGroupRegister()
        async with group_reg:
            # Set up internal arguments queue. We limit its maximum size to enable lazy consumption of `arg_iter` by the
            # `_queue_producer()`; that way an argument
            arg_queue = Queue(maxsize=group_size)
            self._before_gathering.append(join_queue(arg_queue))
            meta_tasks = self._group_meta_tasks_running.setdefault(group_name, set())
            # Start the producer and consumer meta tasks.
            meta_tasks.add(create_task(self._queue_producer(arg_queue, iter(arg_iter), group_name)))
            meta_tasks.add(create_task(self._queue_consumer(arg_queue, group_name, func, arg_stars,
                                                            end_callback, cancel_callback)))

    async def map(self, func: CoroutineFunc, arg_iter: ArgsT, group_size: int = 1, group_name: str = None,
                  end_callback: EndCB = None, cancel_callback: CancelCB = None) -> str:
        """
        An asyncio-task-based equivalent of the `multiprocessing.pool.Pool.map` method.

        Creates coroutines with arguments from the supplied iterable and runs them as new tasks in the pool.
        Each coroutine looks like `func(arg)`, `arg` being an element taken from `arg_iter`.
        All the new tasks are added to the same task group.

        The `group_size` determines the maximum number of tasks spawned this way that shall be running concurrently at
        any given moment in time. As soon as one task from this group ends, it triggers the start of a new task
        (assuming there is room in the pool), which consumes the next element from the arguments iterable. If the size
        of the pool never imposes a limit, this ensures that the number of tasks belonging to this group and running
        concurrently is always equal to `group_size` (except for when `arg_iter` is exhausted of course).

        This method sets up an internal arguments queue which is continuously filled while consuming the `arg_iter`.
        Because this method delegates the spawning of the tasks to two meta tasks (a producer and a consumer of the
        aforementioned queue), it **never blocks**. However, just because this method returns immediately, this does
        not mean that any task was started or that any number of tasks will start soon, as this is solely determined by
        the `pool_size` and the `group_size`.

        Args:
            func:
                The coroutine function to use for spawning the new tasks within the task pool.
            arg_iter:
                The iterable of arguments; each argument is to be passed into a `func` call when spawning a new task.
            group_size (optional):
                The maximum number new tasks spawned by this method to run concurrently. Defaults to 1.
            group_name (optional):
                Name of the task group to add the new tasks to. If provided, it must be a name that doesn't exist yet.
            end_callback (optional):
                A callback to execute after a task has ended.
                It is run with the task's ID as its only positional argument.
            cancel_callback (optional):
                A callback to execute after cancellation of a task.
                It is run with the task's ID as its only positional argument.

        Returns:
            The name of the task group that the newly spawned tasks will be added to.

        Raises:
            `PoolIsClosed` if the pool is closed.
            `NotCoroutine` if `func` is not a coroutine function.
            `PoolIsLocked` if the pool has been locked.
            `ValueError` if `group_size` is less than 1.
            `InvalidGroupName` if a group named `group_name` exists in the pool.
        """
        if group_name is None:
            group_name = self._generate_group_name('map', func)
        await self._map(group_name, group_size, func, arg_iter, 0,
                        end_callback=end_callback, cancel_callback=cancel_callback)
        return group_name

    async def starmap(self, func: CoroutineFunc, args_iter: Iterable[ArgsT], group_size: int = 1,
                      group_name: str = None, end_callback: EndCB = None, cancel_callback: CancelCB = None) -> str:
        """
        Like `map()` except that the elements of `args_iter` are expected to be iterables themselves to be unpacked as
        positional arguments to the function.
        Each coroutine then looks like `func(*args)`, `args` being an element from `args_iter`.
        """
        if group_name is None:
            group_name = self._generate_group_name('starmap', func)
        await self._map(group_name, group_size, func, args_iter, 1,
                        end_callback=end_callback, cancel_callback=cancel_callback)
        return group_name

    async def doublestarmap(self, func: CoroutineFunc, kwargs_iter: Iterable[KwArgsT], group_size: int = 1,
                            group_name: str = None, end_callback: EndCB = None,
                            cancel_callback: CancelCB = None) -> str:
        """
        Like `map()` except that the elements of `kwargs_iter` are expected to be iterables themselves to be unpacked as
        keyword-arguments to the function.
        Each coroutine then looks like `func(**kwargs)`, `kwargs` being an element from `kwargs_iter`.
        """
        if group_name is None:
            group_name = self._generate_group_name('doublestarmap', func)
        await self._map(group_name, group_size, func, kwargs_iter, 2,
                        end_callback=end_callback, cancel_callback=cancel_callback)
        return group_name


class SimpleTaskPool(BaseTaskPool):
    """
    Simplified task pool class.

    A `SimpleTaskPool` instance can manage an arbitrary number of concurrent tasks,
    but they **must** come from a single coroutine function, called with the same arguments.

    The coroutine function and its arguments are defined upon initialization.

    As long as there is room in the pool, more tasks can be added. (By default, there is no pool size limit.)
    Each task started in the pool receives a unique ID, which can be used to cancel specific tasks at any moment.
    However, since all tasks come from the same function-arguments-combination, the specificity of the `cancel()` method
    is probably unnecessary. Instead, a simpler `stop()` method is introduced.

    Adding tasks blocks **only if** the pool is full at that moment.
    """

    def __init__(self, func: CoroutineFunc, args: ArgsT = (), kwargs: KwArgsT = None,
                 end_callback: EndCB = None, cancel_callback: CancelCB = None,
                 pool_size: int = inf, name: str = None) -> None:
        """

        Args:
            func:
                The function to use for spawning new tasks within the pool.
            args (optional):
                The positional arguments to pass into each function call.
            kwargs (optional):
                The keyword-arguments to pass into each function call.
            end_callback (optional):
                A callback to execute after a task has ended.
                It is run with the task's ID as its only positional argument.
            cancel_callback (optional):
                A callback to execute after cancellation of a task.
                It is run with the task's ID as its only positional argument.
            pool_size (optional):
                The maximum number of tasks allowed to run concurrently in the pool
            name (optional):
                An optional name for the pool.
        """
        if not iscoroutinefunction(func):
            raise exceptions.NotCoroutine(f"Not a coroutine function: {func}")
        self._func: CoroutineFunc = func
        self._args: ArgsT = args
        self._kwargs: KwArgsT = kwargs if kwargs is not None else {}
        self._end_callback: EndCB = end_callback
        self._cancel_callback: CancelCB = cancel_callback
        super().__init__(pool_size=pool_size, name=name)

    @property
    def func_name(self) -> str:
        """Returns the name of the coroutine function used in the pool."""
        return self._func.__name__

    async def _start_one(self) -> int:
        """Starts a single new task within the pool and returns its ID."""
        return await self._start_task(self._func(*self._args, **self._kwargs),
                                      end_callback=self._end_callback, cancel_callback=self._cancel_callback)

    async def start(self, num: int = 1) -> List[int]:
        """Starts `num` new tasks within the pool and returns their IDs as a list."""
        ids = await gather(*(self._start_one() for _ in range(num)))
        assert isinstance(ids, list)  # for PyCharm (see above to-do-item)
        return ids

    def stop(self, num: int = 1) -> List[int]:
        """
        Cancels `num` running tasks within the pool and returns their IDs as a list.

        The tasks are canceled in LIFO order, meaning tasks started later will be stopped before those started earlier.
        If `num` is greater than or equal to the number of currently running tasks, naturally all tasks are cancelled.
        """
        ids = []
        for i, task_id in enumerate(reversed(self._tasks_running)):
            if i >= num:
                break  # We got the desired number of task IDs, there may well be more tasks left to keep running
            ids.append(task_id)
        self.cancel(*ids)
        return ids

    def stop_all(self) -> List[int]:
        """Cancels all running tasks and returns their IDs as a list."""
        return self.stop(self.num_running)
