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
from asyncio import gather
from asyncio.coroutines import iscoroutine, iscoroutinefunction
from asyncio.exceptions import CancelledError
from asyncio.locks import Event, Semaphore
from asyncio.queues import Queue, QueueEmpty
from asyncio.tasks import Task, create_task
from functools import partial
from math import inf
from typing import Any, Awaitable, Dict, Iterable, Iterator, List

from . import exceptions
from .helpers import execute_optional, star_function, join_queue
from .types import ArgsT, KwArgsT, CoroutineFunc, EndCallbackT, CancelCallbackT


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
        self._enough_room: Semaphore = Semaphore()
        self.pool_size = pool_size
        self._locked: bool = False
        self._counter: int = 0
        self._running: Dict[int, Task] = {}
        self._cancelled: Dict[int, Task] = {}
        self._ended: Dict[int, Task] = {}
        self._num_cancelled: int = 0
        self._num_ended: int = 0
        self._idx: int = self._add_pool(self)
        self._name: str = name
        self._before_gathering: List[Awaitable] = []
        self._interrupt_flag: Event = Event()
        log.debug("%s initialized", str(self))

    def __str__(self) -> str:
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
        At the moment a task's `end_callback` is fired, it is no longer considered to be running.
        """
        return len(self._running)

    @property
    def num_cancelled(self) -> int:
        """
        Returns the number of tasks in the pool that have been cancelled through the pool (up until that moment).
        At the moment a task's `cancel_callback` is fired, it is considered cancelled and no longer running.
        """
        return self._num_cancelled

    @property
    def num_ended(self) -> int:
        """
        Returns the number of tasks started through the pool that have stopped running (up until that moment).
        At the moment a task's `end_callback` is fired, it is considered ended.
        When a task is cancelled, it is not immediately considered ended; only after its `cancel_callback` has returned,
        does it then actually end.
        """
        return self._num_ended

    @property
    def num_finished(self) -> int:
        """
        Returns the number of tasks in the pool that have actually finished running (without having been cancelled).
        """
        return self._num_ended - self._num_cancelled + len(self._cancelled)

    @property
    def is_full(self) -> bool:
        """
        Returns `False` only if (at that moment) the number of running tasks is below the pool's specified size.
        When the pool is full, any call to start a new task within it will block.
        """
        return self._enough_room.locked()

    # TODO: Consider adding task group names
    def _task_name(self, task_id: int) -> str:
        """Returns a standardized name for a task with a specific `task_id`."""
        return f'{self}_Task-{task_id}'

    async def _task_cancellation(self, task_id: int, custom_callback: CancelCallbackT = None) -> None:
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
        self._cancelled[task_id] = self._running.pop(task_id)
        self._num_cancelled += 1
        log.debug("Cancelled %s", self._task_name(task_id))
        await execute_optional(custom_callback, args=(task_id,))

    async def _task_ending(self, task_id: int, custom_callback: EndCallbackT = None) -> None:
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
            self._ended[task_id] = self._running.pop(task_id)
        except KeyError:
            self._ended[task_id] = self._cancelled.pop(task_id)
        self._num_ended += 1
        self._enough_room.release()
        log.info("Ended %s", self._task_name(task_id))
        await execute_optional(custom_callback, args=(task_id,))

    async def _task_wrapper(self, awaitable: Awaitable, task_id: int, end_callback: EndCallbackT = None,
                            cancel_callback: CancelCallbackT = None) -> Any:
        """
        Universal wrapper around every task to be run in the pool.
        Returns/raises whatever the wrapped coroutine does.

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

    async def _start_task(self, awaitable: Awaitable, ignore_lock: bool = False, end_callback: EndCallbackT = None,
                          cancel_callback: CancelCallbackT = None) -> int:
        """
        Starts a coroutine as a new task in the pool.
        This method blocks, **only if** the pool is full.
        Returns/raises whatever the wrapped coroutine does.

        Args:
            awaitable:
                The actual coroutine to be run within the task pool.
            ignore_lock (optional):
                If `True`, even if the pool is locked, the task will still be started.
            end_callback (optional):
                A callback to execute after the task has ended.
                It is run with the task's ID as its only positional argument.
            cancel_callback (optional):
                A callback to execute after cancellation of the task.
                It is run with the task's ID as its only positional argument.

        Raises:
            `asyncio_taskpool.exceptions.NotCoroutine` if `awaitable` is not a coroutine.
            `asyncio_taskpool.exceptions.PoolIsLocked` if the pool has been locked and `ignore_lock` is `False`.
        """
        if not iscoroutine(awaitable):
            raise exceptions.NotCoroutine(f"Not awaitable: {awaitable}")
        if self._locked and not ignore_lock:
            raise exceptions.PoolIsLocked("Cannot start new tasks")
        await self._enough_room.acquire()
        task_id = self._counter
        self._counter += 1
        try:
            self._running[task_id] = create_task(
                self._task_wrapper(awaitable, task_id, end_callback, cancel_callback),
                name=self._task_name(task_id)
            )
        except Exception as e:
            self._enough_room.release()
            raise e
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
            return self._running[task_id]
        except KeyError:
            if self._cancelled.get(task_id):
                raise exceptions.AlreadyCancelled(f"{self._task_name(task_id)} has already been cancelled")
            if self._ended.get(task_id):
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
        Note that once a pool has been flushed, any IDs of tasks that have ended previously will be forgotten.

        Args:
            task_ids:
                Arbitrary number of integers. Each must be an ID of a task still running within the pool.
            msg (optional):
                Passed to the `Task.cancel()` method of every task specified by the `task_ids`.
        """
        tasks = [self._get_running_task(task_id) for task_id in task_ids]
        for task in tasks:
            task.cancel(msg=msg)

    def cancel_all(self, msg: str = None) -> None:
        """
        Cancels all tasks still running within the pool.

        Note that there may be an unknown number of coroutine functions "queued" to be run as tasks.
        This can happen, if for example the `TaskPool.map` method was called with `num_tasks` set to a number smaller
        than the number of arguments from `args_iter`.
        In this case, those already running will be cancelled, while the following will **never even start**.

        Args:
            msg (optional):
                Passed to the `Task.cancel()` method of every task specified by the `task_ids`.
        """
        log.warning("%s cancelling all tasks!", str(self))
        self._interrupt_flag.set()
        for task in self._running.values():
            task.cancel(msg=msg)

    async def flush(self, return_exceptions: bool = False):
        """
        Calls `asyncio.gather` on all ended/cancelled tasks from the pool, returns their results, and forgets the tasks.
        This method blocks, **only if** any of the tasks block while catching a `asyncio.CancelledError` or any of the
        callbacks registered for the tasks block.

        Args:
            return_exceptions (optional): Passed directly into `gather`.
        """
        results = await gather(*self._ended.values(), *self._cancelled.values(), return_exceptions=return_exceptions)
        self._ended.clear()
        self._cancelled.clear()
        if self._interrupt_flag.is_set():
            self._interrupt_flag.clear()
        return results

    async def gather(self, return_exceptions: bool = False):
        """
        Calls `asyncio.gather` on **all** tasks from the pool, returns their results, and forgets the tasks.

        The `lock()` method must have been called prior to this.

        Note that there may be an unknown number of coroutine functions "queued" to be run as tasks.
        This can happen, if for example the `TaskPool.map` method was called with `num_tasks` set to a number smaller
        than the number of arguments from `args_iter`.
        In this case, calling `cancel_all()` prior to this, will prevent those tasks from starting and potentially
        blocking this method. Otherwise it will wait until they all have started.

        This method may also block, if any task blocks while catching a `asyncio.CancelledError` or if any of the
        callbacks registered for a task blocks.

        Args:
            return_exceptions (optional): Passed directly into `gather`.

        Raises:
            `asyncio_taskpool.exceptions.PoolStillUnlocked` if the pool has not been locked yet.
        """
        if not self._locked:
            raise exceptions.PoolStillUnlocked("Pool must be locked, before tasks can be gathered")
        await gather(*self._before_gathering)
        results = await gather(*self._ended.values(), *self._cancelled.values(), *self._running.values(),
                               return_exceptions=return_exceptions)
        self._ended.clear()
        self._cancelled.clear()
        self._running.clear()
        self._before_gathering.clear()
        if self._interrupt_flag.is_set():
            self._interrupt_flag.clear()
        return results


class TaskPool(BaseTaskPool):
    """
    General task pool class.
    Attempts to somewhat emulate part of the interface of `multiprocessing.pool.Pool` from the stdlib.

    A `TaskPool` instance can manage an arbitrary number of concurrent tasks from any coroutine function.
    Tasks in the pool can all belong to the same coroutine function,
    but they can also come from any number of different and unrelated coroutine functions.

    As long as there is room in the pool, more tasks can be added. (By default, there is no pool size limit.)
    Each task started in the pool receives a unique ID, which can be used to cancel specific tasks at any moment.

    Adding tasks blocks **only if** the pool is full at that moment.
    """

    async def _apply_one(self, func: CoroutineFunc, args: ArgsT = (), kwargs: KwArgsT = None,
                         end_callback: EndCallbackT = None, cancel_callback: CancelCallbackT = None) -> int:
        """
        Creates a coroutine with the supplied arguments and runs it as a new task in the pool.

        This method blocks, **only if** the pool is full.

        Args:
            func:
                The coroutine function to be run as a task within the task pool.
            args (optional):
                The positional arguments to pass into the function call.
            kwargs (optional):
                The keyword-arguments to pass into the function call.
            end_callback (optional):
                A callback to execute after the task has ended.
                It is run with the task's ID as its only positional argument.
            cancel_callback (optional):
                A callback to execute after cancellation of the task.
                It is run with the task's ID as its only positional argument.

        Returns:
            The newly spawned task's ID within the pool.
        """
        if kwargs is None:
            kwargs = {}
        return await self._start_task(func(*args, **kwargs), end_callback=end_callback, cancel_callback=cancel_callback)

    async def apply(self, func: CoroutineFunc, args: ArgsT = (), kwargs: KwArgsT = None, num: int = 1,
                    end_callback: EndCallbackT = None, cancel_callback: CancelCallbackT = None) -> List[int]:
        """
        Creates an arbitrary number of coroutines with the supplied arguments and runs them as new tasks in the pool.
        Each coroutine looks like `func(*args, **kwargs)`.

        This method blocks, **only if** there is not enough room in the pool for the desired number of new tasks.

        Args:
            func:
                The coroutine function to use for spawning the new tasks within the task pool.
            args (optional):
                The positional arguments to pass into each function call.
            kwargs (optional):
                The keyword-arguments to pass into each function call.
            num (optional):
                The number of tasks to spawn with the specified parameters.
            end_callback (optional):
                A callback to execute after a task has ended.
                It is run with the task's ID as its only positional argument.
            cancel_callback (optional):
                A callback to execute after cancellation of a task.
                It is run with the task's ID as its only positional argument.

        Returns:
            The newly spawned tasks' IDs within the pool as a list of integers.

        Raises:
            `NotCoroutine` if `func` is not a coroutine function.
            `PoolIsLocked` if the pool has been locked already.
        """
        ids = await gather(*(self._apply_one(func, args, kwargs, end_callback, cancel_callback) for _ in range(num)))
        # TODO: for some reason PyCharm wrongly claims that `gather` returns a tuple of exceptions
        assert isinstance(ids, list)
        return ids

    async def _queue_producer(self, q: Queue, args_iter: Iterator[Any]) -> None:
        """
        Keeps the arguments queue from `_map()` full as long as the iterator has elements.
        If the `_interrupt_flag` gets set, the loop ends prematurely.

        Args:
            q:
                The queue of function arguments to consume for starting the next task.
            args_iter:
                The iterator of function arguments to put into the queue.
        """
        for arg in args_iter:
            if self._interrupt_flag.is_set():
                break
            await q.put(arg)  # This blocks as long as the queue is full.

    async def _queue_consumer(self, q: Queue, first_batch_started: Event, func: CoroutineFunc, arg_stars: int = 0,
                              end_callback: EndCallbackT = None, cancel_callback: CancelCallbackT = None) -> None:
        """
        Wrapper around the `_start_task()` taking the next element from the arguments queue set up in `_map()`.
        Partially constructs the `_queue_callback` function with the same arguments.

        Args:
            q:
                The queue of function arguments to consume for starting the next task.
            first_batch_started:
                The event flag to wait for, before launching the next consumer.
                It can only set by the `_map()` method, which happens after the first batch of task has been started.
            func:
                The coroutine function to use for spawning the tasks within the task pool.
            arg_stars (optional):
                Whether or not to unpack an element from `q` using stars; must be 0, 1, or 2.
            end_callback (optional):
                The actual callback specified to execute after the task (and the next one) has ended.
                It is run with the task's ID as its only positional argument.
            cancel_callback (optional):
                The callback that was specified to execute after cancellation of the task (and the next one).
                It is run with the task's ID as its only positional argument.
        """
        try:
            arg = q.get_nowait()
        except QueueEmpty:
            return
        try:
            await self._start_task(
                star_function(func, arg, arg_stars=arg_stars),
                ignore_lock=True,
                end_callback=partial(TaskPool._queue_callback, self, q=q, first_batch_started=first_batch_started,
                                     func=func, arg_stars=arg_stars, end_callback=end_callback,
                                     cancel_callback=cancel_callback),
                cancel_callback=cancel_callback
            )
        finally:
            q.task_done()

    async def _queue_callback(self, task_id: int, q: Queue, first_batch_started: Event, func: CoroutineFunc,
                              arg_stars: int = 0, end_callback: EndCallbackT = None,
                              cancel_callback: CancelCallbackT = None) -> None:
        """
        Wrapper around an end callback function passed into the `_map()` method.
        Triggers the next `_queue_consumer` with the same arguments.

        Args:
            task_id:
                The ID of the ending task.
            q:
                The queue of function arguments to consume for starting the next task.
            first_batch_started:
                The event flag to wait for, before launching the next consumer.
                It can only set by the `_map()` method, which happens after the first batch of task has been started.
            func:
                The coroutine function to use for spawning the tasks within the task pool.
            arg_stars (optional):
                Whether or not to unpack an element from `q` using stars; must be 0, 1, or 2.
            end_callback (optional):
                The actual callback specified to execute after the task (and the next one) has ended.
                It is run with the `task_id` as its only positional argument.
            cancel_callback (optional):
                The callback that was specified to execute after cancellation of the task (and the next one).
                It is run with the `task_id` as its only positional argument.
        """
        await first_batch_started.wait()
        await self._queue_consumer(q, first_batch_started, func, arg_stars,
                                   end_callback=end_callback, cancel_callback=cancel_callback)
        await execute_optional(end_callback, args=(task_id,))

    def _set_up_args_queue(self, args_iter: ArgsT, num_tasks: int) -> Queue:
        """
        Helper function for `_map()`.
        Takes the iterable of function arguments `args_iter` and adds up to `num_tasks` to a new `asyncio.Queue`.
        The queue's `join()` method is added to the pool's `_before_gathering` list and the queue is returned.

        If the iterable contains less than `num_tasks` elements, nothing else happens; otherwise the `_queue_producer`
        is started as a separate task with the arguments queue and and iterator of the remaining arguments.

        Args:
            args_iter:
                The iterable of function arguments passed into `_map()` to use for creating the new tasks.
            num_tasks:
                The maximum number of the new tasks to run concurrently that was passed into `_map()`.

        Returns:
            The newly created and filled arguments queue for spawning new tasks.
        """
        # Setting the `maxsize` of the queue to `num_tasks` will ensure that no more than `num_tasks` tasks will run
        # concurrently because the size of the queue is what will determine the number of immediately started tasks in
        # the `_map()` method and each of those will only ever start (at most) one other task upon ending.
        args_queue = Queue(maxsize=num_tasks)
        self._before_gathering.append(join_queue(args_queue))
        args_iter = iter(args_iter)
        try:
            # Here we guarantee that the queue will contain as many arguments as needed for starting the first batch of
            # tasks, which will be at most `num_tasks` (meaning the queue will be full).
            for i in range(num_tasks):
                args_queue.put_nowait(next(args_iter))
        except StopIteration:
            # If we get here, this means that the number of elements in the arguments iterator was less than the
            # specified `num_tasks`. Still, the number of tasks to start immediately will be the size of the queue.
            # The `_queue_producer` won't be necessary, since we already put all the elements in the queue.
            pass
        else:
            # There may be more elements in the arguments iterator, so we need the `_queue_producer`.
            # It will have exclusive access to the `args_iter` from now on.
            # Since the queue is full already, it will wait until one of the tasks in the first batch ends,
            # before putting the next item in it.
            create_task(self._queue_producer(args_queue, args_iter))
        return args_queue

    async def _map(self, func: CoroutineFunc, args_iter: ArgsT, arg_stars: int = 0, num_tasks: int = 1,
                   end_callback: EndCallbackT = None, cancel_callback: CancelCallbackT = None) -> None:
        """
        Creates coroutines with arguments from a supplied iterable and runs them as new tasks in the pool in batches.
        TODO: If task groups are implemented, consider adding all tasks from one call of this method to the same group
              and referring to "group size" rather than chunk/batch size.
        Each coroutine looks like `func(arg)`, `func(*arg)`, or `func(**arg)`, `arg` being an element from the iterable.

        This method blocks, **only if** there is not enough room in the pool for the first batch of new tasks.

        It sets up an internal arguments queue which is continuously filled while consuming the arguments iterable.

        Args:
            func:
                The coroutine function to use for spawning the new tasks within the task pool.
            args_iter:
                The iterable of arguments; each element is to be passed into a `func` call when spawning a new task.
            arg_stars (optional):
                Whether or not to unpack an element from `args_iter` using stars; must be 0, 1, or 2.
            num_tasks (optional):
                The maximum number of the new tasks to run concurrently.
            end_callback (optional):
                A callback to execute after a task has ended.
                It is run with the task's ID as its only positional argument.
            cancel_callback (optional):
                A callback to execute after cancellation of a task.
                It is run with the task's ID as its only positional argument.

        Raises:
            `asyncio_taskpool.exceptions.PoolIsLocked` if the pool has been locked.
        """
        if not self._locked:
            raise exceptions.PoolIsLocked("Cannot start new tasks")
        args_queue = self._set_up_args_queue(args_iter, num_tasks)
        # We need a flag to ensure that starting all tasks from the first batch here will not be blocked by the
        # `_queue_callback` triggered by one or more of them.
        # This could happen, e.g. if the pool has just enough room for one more task, but the queue here contains more
        # than one element, and the pool remains full until after the first task of the first batch ends. Then the
        # callback might trigger the next `_queue_consumer` before this method can, which will keep it blocked.
        first_batch_started = Event()
        for _ in range(args_queue.qsize()):
            # This is where blocking can occur, if the pool is full.
            await self._queue_consumer(args_queue, first_batch_started, func,
                                       arg_stars=arg_stars, end_callback=end_callback, cancel_callback=cancel_callback)
        # Now the callbacks can immediately trigger more tasks.
        first_batch_started.set()

    async def map(self, func: CoroutineFunc, arg_iter: ArgsT, num_tasks: int = 1,
                  end_callback: EndCallbackT = None, cancel_callback: CancelCallbackT = None) -> None:
        """
        An asyncio-task-based equivalent of the `multiprocessing.pool.Pool.map` method.

        Creates coroutines with arguments from a supplied iterable and runs them as new tasks in the pool in batches.
        Each coroutine looks like `func(arg)`, `arg` being an element from the iterable.

        Once the first batch of tasks has started to run, this method returns.
        As soon as on of them finishes, it triggers the start of a new task (assuming there is room in the pool)
        consuming the next element from the arguments iterable.
        If the size of the pool never imposes a limit, this ensures that there is almost continuously the desired number
        of tasks from this call concurrently running within the pool.

        This method blocks, **only if** there is not enough room in the pool for the first batch of new tasks.

        Args:
            func:
                The coroutine function to use for spawning the new tasks within the task pool.
            arg_iter:
                The iterable of arguments; each argument is to be passed into a `func` call when spawning a new task.
            num_tasks (optional):
                The maximum number of the new tasks to run concurrently.
            end_callback (optional):
                A callback to execute after a task has ended.
                It is run with the task's ID as its only positional argument.
            cancel_callback (optional):
                A callback to execute after cancellation of a task.
                It is run with the task's ID as its only positional argument.

        Raises:
            `PoolIsLocked` if the pool has been locked.
            `NotCoroutine` if `func` is not a coroutine function.
        """
        await self._map(func, arg_iter, arg_stars=0, num_tasks=num_tasks,
                        end_callback=end_callback, cancel_callback=cancel_callback)

    async def starmap(self, func: CoroutineFunc, args_iter: Iterable[ArgsT], num_tasks: int = 1,
                      end_callback: EndCallbackT = None, cancel_callback: CancelCallbackT = None) -> None:
        """
        Like `map()` except that the elements of `args_iter` are expected to be iterables themselves to be unpacked as
        positional arguments to the function.
        Each coroutine then looks like `func(*arg)`, `arg` being an element from `args_iter`.
        """
        await self._map(func, args_iter, arg_stars=1, num_tasks=num_tasks,
                        end_callback=end_callback, cancel_callback=cancel_callback)

    async def doublestarmap(self, func: CoroutineFunc, kwargs_iter: Iterable[KwArgsT], num_tasks: int = 1,
                            end_callback: EndCallbackT = None, cancel_callback: CancelCallbackT = None) -> None:
        """
        Like `map()` except that the elements of `kwargs_iter` are expected to be iterables themselves to be unpacked as
        keyword-arguments to the function.
        Each coroutine then looks like `func(**arg)`, `arg` being an element from `kwargs_iter`.
        """
        await self._map(func, kwargs_iter, arg_stars=2, num_tasks=num_tasks,
                        end_callback=end_callback, cancel_callback=cancel_callback)


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
                 end_callback: EndCallbackT = None, cancel_callback: CancelCallbackT = None,
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
        self._end_callback: EndCallbackT = end_callback
        self._cancel_callback: CancelCallbackT = cancel_callback
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
        for i, task_id in enumerate(reversed(self._running)):
            if i >= num:
                break  # We got the desired number of task IDs, there may well be more tasks left to keep running
            ids.append(task_id)
        self.cancel(*ids)
        return ids

    def stop_all(self) -> List[int]:
        """Cancels all running tasks and returns their IDs as a list."""
        return self.stop(self.num_running)
