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
Definitions of the task pool classes.

The :class:`BaseTaskPool` is a parent class and not intended for direct use.
The :class:`TaskPool` and :class:`SimpleTaskPool` are subclasses intended for direct use.
While the former allows for heterogeneous collections of tasks that can be entirely unrelated to one another, the 
latter requires a preemptive decision about the function **and** its arguments upon initialization and only allows
to dynamically control the **number** of tasks running at any point in time.

For further details about the classes check their respective documentation.
"""


import logging
import warnings
from asyncio.coroutines import iscoroutine, iscoroutinefunction
from asyncio.exceptions import CancelledError
from asyncio.locks import Event, Semaphore
from asyncio.tasks import Task, create_task, gather
from contextlib import suppress
from math import inf
from typing import Any, Awaitable, Dict, Iterable, List, Set, Union

from . import exceptions
from .internals.constants import DEFAULT_TASK_GROUP, PYTHON_BEFORE_39
from .internals.group_register import TaskGroupRegister
from .internals.helpers import execute_optional, star_function
from .internals.types import ArgsT, KwArgsT, CoroutineFunc, EndCB, CancelCB


__all__ = [
    'BaseTaskPool',
    'TaskPool',
    'SimpleTaskPool',
    'AnyTaskPoolT'
]


log = logging.getLogger(__name__)


class BaseTaskPool:
    """The base class for task pools. Not intended to be used directly."""
    _pools: List['BaseTaskPool'] = []

    @classmethod
    def _add_pool(cls, pool: 'BaseTaskPool') -> int:
        """Adds a `pool` to the general list of pools and returns it's index."""
        cls._pools.append(pool)
        return len(cls._pools) - 1

    def __init__(self, pool_size: int = inf, name: str = None) -> None:
        """Initializes the necessary internal attributes and adds the new pool to the general pools list."""
        # Initialize a counter for the total number of tasks started through the pool.
        self._num_started: int = 0

        # Initialize flags; immutably set the name.
        self._locked: bool = False
        self._closed: Event = Event()
        self._name: str = name

        # The following three dictionaries are the actual containers of the tasks controlled by the pool.
        self._tasks_running: Dict[int, Task] = {}
        self._tasks_cancelled: Dict[int, Task] = {}
        self._tasks_ended: Dict[int, Task] = {}

        # These next two attributes act as synchronisation primitives necessary for managing the pool.
        self._enough_room: Semaphore = Semaphore()
        self._task_groups: Dict[str, TaskGroupRegister[int]] = {}

        # Mapping task group names to sets of meta tasks, and a bucket for cancelled meta tasks.
        self._group_meta_tasks_running: Dict[str, Set[Task]] = {}
        self._meta_tasks_cancelled: Set[Task] = set()

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
        """Maximum number of concurrently running tasks allowed in the pool."""
        return getattr(self._enough_room, '_value')

    @pool_size.setter
    def pool_size(self, value: int) -> None:
        """
        Sets the maximum number of concurrently running tasks in the pool.

        NOTE: Increasing the pool size will immediately start tasks that are awaiting enough room to run.

        Args:
            value: A non-negative integer.

        Raises:
            `ValueError`: `value` is less than 0.
        """
        if value < 0:
            raise ValueError("Pool size can not be less than 0")
        self._enough_room._value = value

    @property
    def is_locked(self) -> bool:
        """`True` if the pool has been locked (see below)."""
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
        Number of tasks in the pool that are still running.

        At the moment a task's `end_callback` or `cancel_callback` is fired, it is no longer considered running.
        """
        return len(self._tasks_running)

    @property
    def num_cancelled(self) -> int:
        """
        Number of tasks in the pool that have been cancelled.

        At the moment a task's `cancel_callback` is fired, it is considered to be cancelled and no longer running,
        until its `end_callback` is fired, at which point it is considered ended (instead of cancelled).
        """
        return len(self._tasks_cancelled)

    @property
    def num_ended(self) -> int:
        """
        Number of tasks in the pool that have stopped running.

        At the moment a task's `end_callback` is fired, it is considered ended and no longer running (or cancelled).
        When a task is cancelled, it is not immediately considered ended; only after its `cancel_callback` has returned,
        does it then actually end.
        """
        return len(self._tasks_ended)

    @property
    def is_full(self) -> bool:
        """
        `False` if the number of running tasks is less than the pool size.

        When the pool is full, any call to start a new task within it will block, until there is enough room for it.
        """
        return self._enough_room.locked()

    def get_group_ids(self, *group_names: str) -> Set[int]:
        """
        Returns the set of IDs of all tasks in the specified groups.

        Args:
            *group_names: Each element must be a name of a task group that exists within the pool.

        Returns:
            Set of integers representing the task IDs belonging to the specified groups.

        Raises:
            `InvalidGroupName`: One of the specified`group_names` does not exist in the pool.
        """
        ids = set()
        for name in group_names:
            try:
                ids.update(self._task_groups[name])
            except KeyError:
                raise exceptions.InvalidGroupName(f"No task group named {name} exists in this pool.")
        return ids

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
            `AssertionError`: Both or neither of `awaitable` and `function` were passed.
            `asyncio_taskpool.exceptions.PoolIsClosed`: The pool is closed.
            `asyncio_taskpool.exceptions.NotCoroutine`: `awaitable` is not a cor. / `function` not a cor. func.
            `asyncio_taskpool.exceptions.PoolIsLocked`: The pool has been locked and `ignore_lock` is `False`.
        """
        assert (awaitable is None) != (function is None)
        if awaitable and not iscoroutine(awaitable):
            raise exceptions.NotCoroutine(f"Not awaitable: {awaitable}")
        if function and not iscoroutinefunction(function):
            raise exceptions.NotCoroutine(f"Not a coroutine function: {function}")
        if self._closed.is_set():
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
        Universal wrapper around every task run in the pool.

        Returns/raises whatever the wrapped coroutine does.

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

        This method can block for a significant amount of time, **only if** the pool is full. Otherwise it merely needs
        to acquire the :class:`TaskGroupRegister` lock, which should never be held for a long time.

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
        # TODO: Make sure that cancellation (group or pool) interrupts this method after context switching!
        #       Possibly make use of the task group register for that.
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
            `asyncio_taskpool.exceptions.AlreadyCancelled`: The task with `task_id` has been (recently) cancelled.
            `asyncio_taskpool.exceptions.AlreadyEnded`: The task with `task_id` has ended (recently).
            `asyncio_taskpool.exceptions.InvalidTaskID`: No task with `task_id` is known to the pool.
        """
        try:
            return self._tasks_running[task_id]
        except KeyError:
            if self._tasks_cancelled.get(task_id):
                raise exceptions.AlreadyCancelled(f"{self._task_name(task_id)} has been cancelled")
            if self._tasks_ended.get(task_id):
                raise exceptions.AlreadyEnded(f"{self._task_name(task_id)} has finished running")
            raise exceptions.InvalidTaskID(f"No task with ID {task_id} found in {self}")

    @staticmethod
    def _get_cancel_kw(msg: Union[str, None]) -> Dict[str, str]:
        """
        Returns a dictionary to unpack in a `Task.cancel()` method.

        This method exists to ensure proper compatibility with older Python versions.
        If `msg` is `None`, an empty dictionary is returned.
        If `PYTHON_BEFORE_39` is `True` a warning is issued before returning an empty dictionary.
        Otherwise the keyword dictionary contains the `msg` parameter.
        """
        if msg is None:
            return {}
        if PYTHON_BEFORE_39:
            warnings.warn("Parameter `msg` is not available with Python versions before 3.9 and will be ignored.")
            return {}
        return {'msg': msg}

    def cancel(self, *task_ids: int, msg: str = None) -> None:
        """
        Cancels the tasks with the specified IDs.

        Each task ID must belong to a task still running within the pool.

        Note that once a pool has been flushed (see below), IDs of tasks that have ended previously will be forgotten.

        Args:
            *task_ids: Arbitrary number of integers. Each must be an ID of a task still running within the pool.
            msg (optional): Passed to the `Task.cancel()` method of every task specified by the `task_ids`.

        Raises:
            `AlreadyCancelled`: One of the `task_ids` belongs to a task that has been (recently) cancelled.
            `AlreadyEnded`: One of the `task_ids` belongs to a task that has ended (recently).
            `InvalidTaskID`: One of the `task_ids` is not known to the pool.
        """
        tasks = [self._get_running_task(task_id) for task_id in task_ids]
        kw = self._get_cancel_kw(msg)
        for task in tasks:
            task.cancel(**kw)

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

    def _cancel_and_remove_all_from_group(self, group_name: str, group_reg: TaskGroupRegister, **cancel_kw) -> None:
        """
        Removes all tasks from the specified group and cancels them.

        Does nothing to tasks, that are no longer running.

        Args:
            group_name: The name of the group of tasks that shall be cancelled.
            group_reg: The task group register object containing the task IDs.
            msg (optional): Passed to the `Task.cancel()` method of every task specified by the `task_ids`.
        """
        self._cancel_group_meta_tasks(group_name)
        while group_reg:
            try:
                self._tasks_running[group_reg.pop()].cancel(**cancel_kw)
            except KeyError:
                continue
        log.debug("%s cancelled tasks from group %s", str(self), group_name)

    def cancel_group(self, group_name: str, msg: str = None) -> None:
        """
        Cancels an entire group of tasks.

        The task group is subsequently forgotten by the pool.

        If any methods such launched meta tasks belonging to that group, these meta tasks are cancelled before the
        actual tasks are cancelled. This means that any tasks "queued" to be started by a meta task will
        **never even start**.

        Args:
            group_name: The name of the group of tasks (and meta tasks) that shall be cancelled.
            msg (optional): Passed to the `Task.cancel()` method of every task in the group.

        Raises:
            `InvalidGroupName`: No task group named `group_name` exists in the pool.
        """
        log.debug("%s cancelling tasks in group %s", str(self), group_name)
        try:
            group_reg = self._task_groups.pop(group_name)
        except KeyError:
            raise exceptions.InvalidGroupName(f"No task group named {group_name} exists in this pool.")
        kw = self._get_cancel_kw(msg)
        self._cancel_and_remove_all_from_group(group_name, group_reg, **kw)
        log.debug("%s forgot task group %s", str(self), group_name)

    def cancel_all(self, msg: str = None) -> None:
        """
        Cancels all tasks still running within the pool (including meta tasks).

        If any methods such launched meta tasks belonging to that group, these meta tasks are cancelled before the
        actual tasks are cancelled. This means that any tasks "queued" to be started by a meta task will
        **never even start**.

        Args:
            msg (optional): Passed to the `Task.cancel()` method of every task.
        """
        log.warning("%s cancelling all tasks!", str(self))
        kw = self._get_cancel_kw(msg)
        while self._task_groups:
            group_name, group_reg = self._task_groups.popitem()
            self._cancel_and_remove_all_from_group(group_name, group_reg, **kw)

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
        Gathers (i.e. awaits) all ended/cancelled tasks in the pool.

        The tasks are subsequently forgotten by the pool. This method exists mainly to free up memory of unneeded
        `Task` objects. It also gets rid of unneeded (ended/cancelled) meta tasks.

        It blocks, **only if** any of the tasks block while catching a `asyncio.CancelledError` or any of the callbacks
        registered for the tasks block.

        Args:
            return_exceptions (optional): Passed directly into `gather`.
        """
        with suppress(CancelledError):
            await gather(*self._meta_tasks_cancelled, *self._pop_ended_meta_tasks(),
                         return_exceptions=return_exceptions)
        self._meta_tasks_cancelled.clear()
        await gather(*self._tasks_ended.values(), *self._tasks_cancelled.values(), return_exceptions=return_exceptions)
        self._tasks_ended.clear()
        self._tasks_cancelled.clear()

    async def gather_and_close(self, return_exceptions: bool = False):
        """
        Gathers (i.e. awaits) **all** tasks in the pool, then closes it.

        Once this method is called, no more tasks can be started in the pool.

        Note that this method may block indefinitely as long as any task in the pool is not done. This includes meta
        tasks launched by other methods, which may or may not even end by themselves. To avoid this, make sure to call
        :meth:`cancel_all` first.

        This method may also block, if one of the tasks blocks while catching a `asyncio.CancelledError` or if any of
        the callbacks registered for a task blocks for whatever reason.

        Args:
            return_exceptions (optional): Passed directly into `gather`.

        Raises:
            `PoolStillUnlocked`: The pool has not been locked yet.
        """
        self.lock()
        not_cancelled_meta_tasks = (task for task_set in self._group_meta_tasks_running.values() for task in task_set)
        with suppress(CancelledError):
            await gather(*self._meta_tasks_cancelled, *not_cancelled_meta_tasks, return_exceptions=return_exceptions)
        self._meta_tasks_cancelled.clear()
        self._group_meta_tasks_running.clear()
        await gather(*self._tasks_ended.values(), *self._tasks_cancelled.values(), *self._tasks_running.values(),
                     return_exceptions=return_exceptions)
        self._tasks_ended.clear()
        self._tasks_cancelled.clear()
        self._tasks_running.clear()
        self._closed.set()

    async def until_closed(self) -> bool:
        return await self._closed.wait()


class TaskPool(BaseTaskPool):
    """
    General purpose task pool class.

    Attempts to emulate part of the interface of `multiprocessing.pool.Pool` from the stdlib.

    A `TaskPool` instance can manage an arbitrary number of concurrent tasks from any coroutine function.
    Tasks in the pool can all belong to the same coroutine function,
    but they can also come from any number of different and unrelated coroutine functions.

    As long as there is room in the pool, more tasks can be added. (By default, there is no pool size limit.)
    Each task started in the pool receives a unique ID, which can be used to cancel specific tasks at any moment.

    Adding tasks blocks **only if** the pool is full at that moment.
    """

    def _generate_group_name(self, prefix: str, coroutine_function: CoroutineFunc) -> str:
        """
        Creates a unique task group identifier.

        Args:
            prefix: The start of the name; will be followed by an underscore.
            coroutine_function: The function representing the task group.

        Returns:
            The constructed '{prefix}-{name}-group-{idx}' string to name a task group.
            (With `name` being the name of the `coroutine_function` and `idx` being an incrementing index.)
        """
        base_name = f'{prefix}-{coroutine_function.__name__}-group'
        i = 0
        while True:
            name = f'{base_name}-{i}'
            if name not in self._task_groups.keys():
                return name
            i += 1

    async def _apply_spawner(self, group_name: str, func: CoroutineFunc, args: ArgsT = (), kwargs: KwArgsT = None,
                             num: int = 1, end_callback: EndCB = None, cancel_callback: CancelCB = None) -> None:
        """
        Creates coroutines with the supplied arguments and runs them as new tasks in the pool.

        This method blocks, **only if** the pool has not enough room to accommodate `num` new tasks.

        Args:
            group_name:
                Name of the task group to add the new tasks to.
            func:
                The coroutine function to be run in `num` tasks within the task pool.
            args (optional):
                The positional arguments to pass into each function call.
            kwargs (optional):
                The keyword-arguments to pass into each function call.
            num (optional):
                The number of tasks to spawn with the specified parameters.
            end_callback (optional):
                A callback to execute after each task has ended.
                It is run with the task's ID as its only positional argument.
            cancel_callback (optional):
                A callback to execute after cancellation of each task.
                It is run with the task's ID as its only positional argument.
        """
        if kwargs is None:
            kwargs = {}
        for i in range(num):
            try:
                coroutine = func(*args, **kwargs)
            except Exception as e:
                # This means there was probably something wrong with the function arguments.
                log.exception("%s occurred in group '%s' while trying to create coroutine: %s(*%s, **%s)",
                              str(e.__class__.__name__), group_name, func.__name__, repr(args), repr(kwargs))
                continue  # TODO: Consider returning instead of continuing
            try:
                await self._start_task(coroutine, group_name=group_name, end_callback=end_callback,
                                       cancel_callback=cancel_callback)
            except CancelledError:
                # Either the task group or all tasks were cancelled, so this meta tasks is not supposed to spawn any
                # more tasks and can return immediately.
                log.debug("Cancelled group '%s' after %s out of %s tasks have been spawned", group_name, i, num)
                coroutine.close()
                return

    def apply(self, func: CoroutineFunc, args: ArgsT = (), kwargs: KwArgsT = None, num: int = 1, group_name: str = None,
              end_callback: EndCB = None, cancel_callback: CancelCB = None) -> str:
        """
        Creates tasks with the supplied arguments to be run in the pool.

        Each coroutine looks like `func(*args, **kwargs)`, meaning the `args` and `kwargs` are unpacked and passed
        into `func` before creating each task, and this is done `num` times.

        All the new tasks are added to the same task group.

        Because this method delegates the spawning of the tasks to a meta task, it **never blocks**. However, just
        because this method returns immediately, this does not mean that any task was started or that any number of
        tasks will start soon, as this is solely determined by the :attr:`BaseTaskPool.pool_size` and `num`.

        If the entire task group is cancelled before `num` tasks have spawned, since the meta task is cancelled first,
        the number of tasks spawned will end up being less than `num`.

        Args:
            func:
                The coroutine function to use for spawning the new tasks within the task pool.
            args (optional):
                The positional arguments to pass into each function call.
            kwargs (optional):
                The keyword-arguments to pass into each function call.
            num (optional):
                The number of tasks to spawn with the specified parameters. Defaults to 1.
            group_name (optional):
                Name of the task group to add the new tasks to. By default, a unique name is constructed in the form
                :code:`'apply-{name}-group-{idx}'` (with `name` being the name of the `func` and `idx` being an
                incrementing index).
            end_callback (optional):
                A callback to execute after a task has ended.
                It is run with the task's ID as its only positional argument.
            cancel_callback (optional):
                A callback to execute after cancellation of a task.
                It is run with the task's ID as its only positional argument.

        Returns:
            The name of the newly created group (see the `group_name` parameter).

        Raises:
            `PoolIsClosed`: The pool is closed.
            `NotCoroutine`: `func` is not a coroutine function.
            `PoolIsLocked`: The pool is currently locked.
            `InvalidGroupName`: A group named `group_name` exists in the pool.
        """
        self._check_start(function=func)
        if group_name is None:
            group_name = self._generate_group_name('apply', func)
        if group_name in self._task_groups.keys():
            raise exceptions.InvalidGroupName(f"Group named {group_name} already exists!")
        self._task_groups.setdefault(group_name, TaskGroupRegister())
        meta_tasks = self._group_meta_tasks_running.setdefault(group_name, set())
        meta_tasks.add(create_task(self._apply_spawner(group_name, func, args, kwargs, num,
                                                       end_callback=end_callback, cancel_callback=cancel_callback)))
        return group_name

    @staticmethod
    def _get_map_end_callback(map_semaphore: Semaphore, actual_end_callback: EndCB) -> EndCB:
        """Returns a wrapped `end_callback` for each :meth:`_arg_consumer` task that releases the `map_semaphore`."""
        async def release_callback(task_id: int) -> None:
            map_semaphore.release()
            await execute_optional(actual_end_callback, args=(task_id,))
        return release_callback

    async def _arg_consumer(self, group_name: str, num_concurrent: int, func: CoroutineFunc, arg_iter: ArgsT,
                            arg_stars: int, end_callback: EndCB = None, cancel_callback: CancelCB = None) -> None:
        """
        Consumes arguments from :meth:`_map` and keeps a limited number of tasks working on them.

        `num_concurrent` acts as the limiting value of an internal semaphore, which must be acquired before a new task
        can be started, and which must be released when one of these tasks ends.

        Intended to be run as a meta task of a specific group.

        Args:
            group_name:
                Name of the associated task group; passed into :meth:`_start_task`.
            num_concurrent:
                The maximum number new tasks spawned by this method to run concurrently.
            func:
                The coroutine function to use for spawning the new tasks within the task pool.
            arg_iter:
                The iterable of arguments; each element is to be passed into a `func` call when spawning a new task.
            arg_stars (optional):
                Whether or not to unpack an element from `arg_queue` using stars; must be 0, 1, or 2.
            end_callback (optional):
                The actual callback specified to execute after the task (and the next one) has ended.
                It is run with the task's ID as its only positional argument.
            cancel_callback (optional):
                The callback that was specified to execute after cancellation of the task (and the next one).
                It is run with the task's ID as its only positional argument.
        """
        semaphore = Semaphore(num_concurrent)
        release_cb = self._get_map_end_callback(semaphore, actual_end_callback=end_callback)
        for i, next_arg in enumerate(arg_iter):
            semaphore_acquired = False
            try:
                coroutine = star_function(func, next_arg, arg_stars=arg_stars)
            except Exception as e:
                # This means there was probably something wrong with the function arguments.
                log.exception("%s occurred in group '%s' while trying to create coroutine: %s(%s%s)",
                              str(e.__class__.__name__), group_name, func.__name__, '*' * arg_stars, str(next_arg))
                continue
            try:
                # When the number of running tasks spawned by this method reaches the specified maximum,
                # this next line will block, until one of them ends and releases the semaphore.
                semaphore_acquired = await semaphore.acquire()
                await self._start_task(coroutine, group_name=group_name, ignore_lock=True,
                                       end_callback=release_cb, cancel_callback=cancel_callback)
            except CancelledError:
                # Either the task group or all tasks were cancelled, so this meta tasks is not supposed to spawn any
                # more tasks and can return immediately. (This means we drop `arg_iter` without consuming it fully.)
                log.debug("Cancelled group '%s' after %s tasks have been spawned", group_name, i)
                coroutine.close()
                if semaphore_acquired:
                    semaphore.release()
                return

    def _map(self, group_name: str, num_concurrent: int, func: CoroutineFunc, arg_iter: ArgsT, arg_stars: int,
             end_callback: EndCB = None, cancel_callback: CancelCB = None) -> None:
        """
        Creates tasks in the pool with arguments from the supplied iterable.

        Each coroutine looks like `func(arg)`, `func(*arg)`, or `func(**arg)`, `arg` being taken from `arg_iter`.

        All the new tasks are added to the same task group.

        `num_concurrent` determines the (maximum) number of tasks spawned this way that shall be running concurrently at
        any given moment in time. As soon as one task from this method call ends, it triggers the start of a new task
        (assuming there is room in the pool), which consumes the next element from the arguments iterable. If the size
        of the pool never imposes a limit, this ensures that the number of tasks spawned and running concurrently is
        always equal to `num_concurrent` (except for when `arg_iter` is exhausted of course).

        Because this method delegates the spawning of the tasks to a meta task, it **never blocks**. However, just
        because this method returns immediately, this does not mean that any task was started or that any number of
        tasks will start soon, as this is solely determined by the :attr:`BaseTaskPool.pool_size` and `num_concurrent`.

        If the entire task group is cancelled, the meta task is cancelled first, which means that `arg_iter` may be
        abandoned before being fully consumed (if that is even possible).

        Args:
            group_name:
                Name of the task group to add the new tasks to. It must be a name that doesn't exist yet.
            num_concurrent:
                The number new tasks spawned by this method to run concurrently.
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
            `ValueError`: `num_concurrent` is less than 1.
            `asyncio_taskpool.exceptions.InvalidGroupName`: A group named `group_name` exists in the pool.
        """
        self._check_start(function=func)
        if num_concurrent < 1:
            raise ValueError("`num_concurrent` must be a positive integer.")
        if group_name in self._task_groups.keys():
            raise exceptions.InvalidGroupName(f"Group named {group_name} already exists!")
        self._task_groups[group_name] = TaskGroupRegister()
        meta_tasks = self._group_meta_tasks_running.setdefault(group_name, set())
        meta_tasks.add(create_task(self._arg_consumer(group_name, num_concurrent, func, arg_iter, arg_stars,
                                                      end_callback=end_callback, cancel_callback=cancel_callback)))

    def map(self, func: CoroutineFunc, arg_iter: ArgsT, num_concurrent: int = 1, group_name: str = None,
            end_callback: EndCB = None, cancel_callback: CancelCB = None) -> str:
        """
        A task-based equivalent of the `multiprocessing.pool.Pool.map` method.

        Creates coroutines with arguments from the supplied iterable and runs them as new tasks in the pool.
        Each coroutine looks like `func(arg)`, `arg` being an element taken from `arg_iter`.

        All the new tasks are added to the same task group.

        `num_concurrent` determines the (maximum) number of tasks spawned this way that shall be running concurrently at
        any given moment in time. As soon as one task from this method call ends, it triggers the start of a new task
        (assuming there is room in the pool), which consumes the next element from the arguments iterable. If the size
        of the pool never imposes a limit, this ensures that the number of tasks spawned and running concurrently is
        always equal to `num_concurrent` (except for when `arg_iter` is exhausted of course).

        Because this method delegates the spawning of the tasks to a meta task, it **never blocks**. However, just
        because this method returns immediately, this does not mean that any task was started or that any number of
        tasks will start soon, as this is solely determined by the :attr:`BaseTaskPool.pool_size` and `num_concurrent`.

        If the entire task group is cancelled, the meta task is cancelled first, which means that `arg_iter` may be
        abandoned before being fully consumed (if that is even possible).

        Args:
            func:
                The coroutine function to use for spawning the new tasks within the task pool.
            arg_iter:
                The iterable of arguments; each argument is to be passed into a `func` call when spawning a new task.
            num_concurrent (optional):
                The number new tasks spawned by this method to run concurrently. Defaults to 1.
            group_name (optional):
                Name of the task group to add the new tasks to. If provided, it must be a name that doesn't exist yet.
                By default, a unique name is constructed in the form :code:`'map-{name}-group-{idx}'`
                (with `name` being the name of the `func` and `idx` being an incrementing index).
            end_callback (optional):
                A callback to execute after a task has ended.
                It is run with the task's ID as its only positional argument.
            cancel_callback (optional):
                A callback to execute after cancellation of a task.
                It is run with the task's ID as its only positional argument.

        Returns:
            The name of the newly created group (see the `group_name` parameter).

        Raises:
            `PoolIsClosed`: The pool is closed.
            `NotCoroutine`: `func` is not a coroutine function.
            `PoolIsLocked`: The pool is currently locked.
            `ValueError`: `num_concurrent` is less than 1.
            `InvalidGroupName`: A group named `group_name` exists in the pool.
        """
        if group_name is None:
            group_name = self._generate_group_name('map', func)
        self._map(group_name, num_concurrent, func, arg_iter, 0,
                  end_callback=end_callback, cancel_callback=cancel_callback)
        return group_name

    def starmap(self, func: CoroutineFunc, args_iter: Iterable[ArgsT], num_concurrent: int = 1, group_name: str = None,
                end_callback: EndCB = None, cancel_callback: CancelCB = None) -> str:
        """
        Like :meth:`map` except that the elements of `args_iter` are expected to be iterables themselves to be unpacked
        as positional arguments to the function.
        Each coroutine then looks like `func(*args)`, `args` being an element from `args_iter`.

        Returns:
            The name of the newly created group in the form :code:`'starmap-{name}-group-{index}'`
            (with `name` being the name of the `func` and `idx` being an incrementing index).
        """
        if group_name is None:
            group_name = self._generate_group_name('starmap', func)
        self._map(group_name, num_concurrent, func, args_iter, 1,
                  end_callback=end_callback, cancel_callback=cancel_callback)
        return group_name

    def doublestarmap(self, func: CoroutineFunc, kwargs_iter: Iterable[KwArgsT], num_concurrent: int = 1,
                      group_name: str = None, end_callback: EndCB = None, cancel_callback: CancelCB = None) -> str:
        """
        Like :meth:`map` except that the elements of `kwargs_iter` are expected to be iterables themselves to be
        unpacked as keyword-arguments to the function.
        Each coroutine then looks like `func(**kwargs)`, `kwargs` being an element from `kwargs_iter`.

        Returns:
            The name of the newly created group in the form :code:`'doublestarmap-{name}-group-{index}'`
            (with `name` being the name of the `func` and `idx` being an incrementing index).
        """
        if group_name is None:
            group_name = self._generate_group_name('doublestarmap', func)
        self._map(group_name, num_concurrent, func, kwargs_iter, 2,
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
    is probably unnecessary. Instead, a simpler :meth:`stop` method is introduced.

    Adding tasks blocks **only if** the pool is full at that moment.
    """

    def __init__(self, func: CoroutineFunc, args: ArgsT = (), kwargs: KwArgsT = None,
                 end_callback: EndCB = None, cancel_callback: CancelCB = None,
                 pool_size: int = inf, name: str = None) -> None:
        """
        Initializes all required attributes.

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

        Raises:
            `NotCoroutine`: `func` is not a coroutine function.
        """
        if not iscoroutinefunction(func):
            raise exceptions.NotCoroutine(f"Not a coroutine function: {func}")
        self._func: CoroutineFunc = func
        self._args: ArgsT = args
        self._kwargs: KwArgsT = kwargs if kwargs is not None else {}
        self._end_callback: EndCB = end_callback
        self._cancel_callback: CancelCB = cancel_callback
        self._start_calls: int = 0
        super().__init__(pool_size=pool_size, name=name)

    @property
    def func_name(self) -> str:
        """Name of the coroutine function used in the pool."""
        return self._func.__name__

    async def _start_num(self, num: int, group_name: str) -> None:
        """Starts `num` new tasks in group `group_name`."""
        for i in range(num):
            try:
                coroutine = self._func(*self._args, **self._kwargs)
            except Exception as e:
                # This means there was probably something wrong with the function arguments.
                log.exception("%s occurred in '%s' while trying to create coroutine: %s(*%s, **%s)",
                              str(e.__class__.__name__), str(self), self._func.__name__,
                              repr(self._args), repr(self._kwargs))
                continue  # TODO: Consider returning instead of continuing
            try:
                await self._start_task(coroutine, group_name=group_name, end_callback=self._end_callback,
                                       cancel_callback=self._cancel_callback)
            except CancelledError:
                # Either the task group or all tasks were cancelled, so this meta tasks is not supposed to spawn any
                # more tasks and can return immediately.
                log.debug("Cancelled group '%s' after %s out of %s tasks have been spawned", group_name, i, num)
                coroutine.close()
                return

    def start(self, num: int) -> str:
        """
        Starts specified number of new tasks in the pool as a new group.

        Because this method delegates the spawning of the tasks to a meta task, it **never blocks**. However, just
        because this method returns immediately, this does not mean that any task was started or that any number of
        tasks will start soon, as this is solely determined by the :attr:`BaseTaskPool.pool_size` and `num`.

        If the entire task group is cancelled before `num` tasks have spawned, since the meta task is cancelled first,
        the number of tasks spawned will end up being less than `num`.

        Args:
            num: The number of new tasks to start.

        Returns:
            The name of the newly created task group in the form :code:`'start-group-{idx}'`
            (with `idx` being an incrementing index).
        """
        self._check_start(function=self._func)
        group_name = f'start-group-{self._start_calls}'
        self._start_calls += 1
        self._task_groups.setdefault(group_name, TaskGroupRegister())
        meta_tasks = self._group_meta_tasks_running.setdefault(group_name, set())
        meta_tasks.add(create_task(self._start_num(num, group_name)))
        return group_name

    def stop(self, num: int) -> List[int]:
        """
        Cancels specified number of tasks in the pool and returns their IDs.

        The tasks are canceled in LIFO order, meaning tasks started later will be stopped before those started earlier.

        Args:
            num: The number of tasks to cancel; if `num` >= :attr:`BaseTaskPool.num_running`, all tasks are cancelled.

        Returns:
            List of IDs of the tasks that have been cancelled (in the order they were cancelled).
        """
        ids = []
        for i, task_id in enumerate(reversed(self._tasks_running)):
            if i >= num:
                break  # We got the desired number of task IDs, there may well be more tasks left to keep running
            ids.append(task_id)
        self.cancel(*ids)
        return ids

    def stop_all(self) -> List[int]:
        """Cancels all running tasks and returns their IDs."""
        return self.stop(self.num_running)


AnyTaskPoolT = Union[TaskPool, SimpleTaskPool]
