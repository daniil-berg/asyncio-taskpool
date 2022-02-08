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
        self._open: bool = True
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
    def is_open(self) -> bool:
        """Returns `True` if more the pool has not been closed yet."""
        return self._open

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

    async def _start_task(self, awaitable: Awaitable, ignore_closed: bool = False, end_callback: EndCallbackT = None,
                          cancel_callback: CancelCallbackT = None) -> int:
        """
        Starts a coroutine as a new task in the pool.
        This method blocks, **only if** the pool is full.
        Returns/raises whatever the wrapped coroutine does.

        Args:
            awaitable:
                The actual coroutine to be run within the task pool.
            ignore_closed (optional):
                If `True`, even if the pool is closed, the task will still be started.
            end_callback (optional):
                A callback to execute after the task has ended.
                It is run with the task's ID as its only positional argument.
            cancel_callback (optional):
                A callback to execute after cancellation of the task.
                It is run with the task's ID as its only positional argument.

        Raises:
            `asyncio_taskpool.exceptions.NotCoroutine` if `awaitable` is not a coroutine.
            `asyncio_taskpool.exceptions.PoolIsClosed` if the pool has been closed and `ignore_closed` is `False`.
        """
        if not iscoroutine(awaitable):
            raise exceptions.NotCoroutine(f"Not awaitable: {awaitable}")
        if not (self.is_open or ignore_closed):
            raise exceptions.PoolIsClosed("Cannot start new tasks")
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

    def close(self) -> None:
        """Disallows any more tasks to be started in the pool."""
        self._open = False
        log.info("%s is closed!", str(self))

    async def gather(self, return_exceptions: bool = False):
        """
        Calls `asyncio.gather` on **all** tasks from the pool, returns their results, and forgets the tasks.

        The `close()` method must have been called prior to this.

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
            `asyncio_taskpool.exceptions.PoolStillOpen` if the pool has not been closed yet.
        """
        if self._open:
            raise exceptions.PoolStillOpen("Pool must be closed, before tasks can be gathered")
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
            `PoolIsClosed` if the pool has been closed already.
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

    async def _queue_consumer(self, q: Queue, func: CoroutineFunc, arg_stars: int = 0,
                              end_callback: EndCallbackT = None, cancel_callback: CancelCallbackT = None) -> None:
        """
        Wrapper around the `_start_task()` taking the next element from the arguments queue set up in `_map()`.
        Partially constructs the `_queue_callback` function with the same arguments.

        Args:
            q:
                The queue of function arguments to consume for starting the next task.
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
                ignore_closed=True,
                end_callback=partial(TaskPool._queue_callback, self, q=q, func=func, arg_stars=arg_stars,
                                     end_callback=end_callback, cancel_callback=cancel_callback),
                cancel_callback=cancel_callback
            )
        finally:
            q.task_done()

    async def _queue_callback(self, task_id: int, q: Queue, func: CoroutineFunc, arg_stars: int = 0,
                              end_callback: EndCallbackT = None, cancel_callback: CancelCallbackT = None) -> None:
        """
        Wrapper around an end callback function passed into the `_map()` method.
        Triggers the next `_queue_consumer` with the same arguments.

        Args:
            task_id:
                The ID of the ending task.
            q:
                The queue of function arguments to consume for starting the next task.
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
        await self._queue_consumer(q, func, arg_stars, end_callback=end_callback, cancel_callback=cancel_callback)
        await execute_optional(end_callback, args=(task_id,))

    def _fill_args_queue(self, q: Queue, args_iter: ArgsT, num_tasks: int) -> int:
        args_iter = iter(args_iter)
        try:
            # Here we guarantee that the queue will contain as many arguments as needed for starting the first batch of
            # tasks, which will be at most `num_tasks` (meaning the queue will be full).
            for i in range(num_tasks):
                q.put_nowait(next(args_iter))
        except StopIteration:
            # If we get here, this means that the number of elements in the arguments iterator was less than the
            # specified `num_tasks`. Thus, the number of tasks to start immediately will be the size of the queue.
            # The `_queue_producer` won't be necessary, since we already put all the elements in the queue.
            num_tasks = q.qsize()
        else:
            # There may be more elements in the arguments iterator, so we need the `_queue_producer`.
            # It will have exclusive access to the `args_iter` from now on.
            # If the queue is full already, it will wait until one of the tasks in the first batch ends, before putting
            # the next item in it.
            create_task(self._queue_producer(q, args_iter))
        return num_tasks

    async def _map(self, func: CoroutineFunc, args_iter: ArgsT, arg_stars: int = 0, num_tasks: int = 1,
                   end_callback: EndCallbackT = None, cancel_callback: CancelCallbackT = None) -> None:
        """
        Creates coroutines with arguments from a supplied iterable and runs them as new tasks in the pool in batches.
        TODO: If task groups are implemented, consider adding all tasks from one call of this method to the same group
              and referring to "group size" rather than chunk/batch size.
        Each coroutine looks like `func(arg)`, `func(*arg)`, or `func(**arg)`, `arg` being an element from the iterable.

        This method blocks, **only if** there is not enough room in the pool for the first batch of new tasks.

        It sets up an internal queue which is filled while consuming the arguments iterable.
        The queue's `join()` method is added to the pool's `_before_gathering` list.

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
            `asyncio_taskpool.exceptions.PoolIsClosed` if the pool has been closed.
        """
        if not self.is_open:
            raise exceptions.PoolIsClosed("Cannot start new tasks")
        args_queue = Queue(maxsize=num_tasks)
        self._before_gathering.append(join_queue(args_queue))
        num_tasks = self._fill_args_queue(args_queue, args_iter, num_tasks)
        for _ in range(num_tasks):
            # This is where blocking can occur, if the pool is full.
            await self._queue_consumer(args_queue, func,
                                       arg_stars=arg_stars, end_callback=end_callback, cancel_callback=cancel_callback)

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
            `PoolIsClosed` if the pool has been closed.
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
    def __init__(self, func: CoroutineFunc, args: ArgsT = (), kwargs: KwArgsT = None,
                 end_callback: EndCallbackT = None, cancel_callback: CancelCallbackT = None,
                 name: str = None) -> None:
        if not iscoroutinefunction(func):
            raise exceptions.NotCoroutine(f"Not a coroutine function: {func}")
        self._func: CoroutineFunc = func
        self._args: ArgsT = args
        self._kwargs: KwArgsT = kwargs if kwargs is not None else {}
        self._end_callback: EndCallbackT = end_callback
        self._cancel_callback: CancelCallbackT = cancel_callback
        super().__init__(name=name)

    @property
    def func_name(self) -> str:
        return self._func.__name__

    @property
    def size(self) -> int:
        return self.num_running

    async def _start_one(self) -> int:
        return await self._start_task(self._func(*self._args, **self._kwargs),
                                      end_callback=self._end_callback, cancel_callback=self._cancel_callback)

    async def start(self, num: int = 1) -> List[int]:
        return [await self._start_one() for _ in range(num)]

    def stop(self, num: int = 1) -> List[int]:
        num = min(num, self.size)
        ids = []
        for i, task_id in enumerate(reversed(self._running)):
            if i >= num:
                break
            ids.append(task_id)
        self.cancel(*ids)
        return ids

    def stop_all(self) -> List[int]:
        return self.stop(self.size)
