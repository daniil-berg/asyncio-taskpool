import logging
from asyncio import gather
from asyncio.coroutines import iscoroutinefunction
from asyncio.exceptions import CancelledError
from asyncio.locks import Event, Semaphore
from asyncio.tasks import Task, create_task
from math import inf
from typing import Any, Awaitable, Callable, Dict, Iterable, Iterator, List, Optional, Tuple

from . import exceptions
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
        self._all_tasks_known_flag: Event = Event()
        self._all_tasks_known_flag.set()
        log.debug("%s initialized", str(self))

    def __str__(self) -> str:
        return f'{self.__class__.__name__}-{self._name or self._idx}'

    @property
    def pool_size(self) -> int:
        return self._pool_size

    @pool_size.setter
    def pool_size(self, value: int) -> None:
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
        log.debug("Cancelling %s ...", self._task_name(task_id))
        self._cancelled[task_id] = self._running.pop(task_id)
        self._num_cancelled += 1
        log.debug("Cancelled %s", self._task_name(task_id))
        await _execute_function(custom_callback, args=(task_id, ))

    async def _task_ending(self, task_id: int, custom_callback: EndCallbackT = None) -> None:
        try:
            self._ended[task_id] = self._running.pop(task_id)
        except KeyError:
            self._ended[task_id] = self._cancelled.pop(task_id)
        self._num_ended += 1
        self._enough_room.release()
        log.info("Ended %s", self._task_name(task_id))
        await _execute_function(custom_callback, args=(task_id, ))

    async def _task_wrapper(self, awaitable: Awaitable, task_id: int, end_callback: EndCallbackT = None,
                            cancel_callback: CancelCallbackT = None) -> Any:
        log.info("Started %s", self._task_name(task_id))
        try:
            return await awaitable
        except CancelledError:
            await self._task_cancellation(task_id, custom_callback=cancel_callback)
        finally:
            await self._task_ending(task_id, custom_callback=end_callback)

    async def _start_task(self, awaitable: Awaitable, ignore_closed: bool = False, end_callback: EndCallbackT = None,
                          cancel_callback: CancelCallbackT = None) -> int:
        if not (self.is_open or ignore_closed):
            raise exceptions.PoolIsClosed("Cannot start new tasks")
        await self._enough_room.acquire()
        try:
            task_id = self._counter
            self._counter += 1
            self._running[task_id] = create_task(
                self._task_wrapper(awaitable, task_id, end_callback, cancel_callback),
                name=self._task_name(task_id)
            )
        except Exception as e:
            self._enough_room.release()
            raise e
        return task_id

    def _cancel_one(self, task_id: int, msg: str = None) -> None:
        try:
            task = self._running[task_id]
        except KeyError:
            if self._cancelled.get(task_id):
                raise exceptions.AlreadyCancelled(f"{self._task_name(task_id)} has already been cancelled")
            if self._ended.get(task_id):
                raise exceptions.AlreadyFinished(f"{self._task_name(task_id)} has finished running")
            raise exceptions.InvalidTaskID(f"No task with ID {task_id} found in {self}")
        task.cancel(msg=msg)

    def cancel(self, *task_ids: int, msg: str = None) -> None:
        for task_id in task_ids:
            self._cancel_one(task_id, msg=msg)

    def cancel_all(self, msg: str = None) -> None:
        for task in self._running.values():
            task.cancel(msg=msg)

    async def flush(self, return_exceptions: bool = False):
        results = await gather(*self._ended.values(), *self._cancelled.values(), return_exceptions=return_exceptions)
        self._ended = self._cancelled = {}
        return results

    def close(self) -> None:
        self._open = False
        log.info("%s is closed!", str(self))

    async def gather(self, return_exceptions: bool = False):
        if self._open:
            raise exceptions.PoolStillOpen("Pool must be closed, before tasks can be gathered")
        await self._all_tasks_known_flag.wait()
        results = await gather(*self._ended.values(), *self._cancelled.values(), *self._running.values(),
                               return_exceptions=return_exceptions)
        self._ended = self._cancelled = self._running = {}
        return results


class TaskPool(BaseTaskPool):
    async def _apply_one(self, func: CoroutineFunc, args: ArgsT = (), kwargs: KwArgsT = None,
                         end_callback: EndCallbackT = None, cancel_callback: CancelCallbackT = None) -> int:
        if kwargs is None:
            kwargs = {}
        return await self._start_task(func(*args, **kwargs), end_callback=end_callback, cancel_callback=cancel_callback)

    async def apply(self, func: CoroutineFunc, args: ArgsT = (), kwargs: KwArgsT = None, num: int = 1,
                    end_callback: EndCallbackT = None, cancel_callback: CancelCallbackT = None) -> Tuple[int]:
        return tuple(await self._apply_one(func, args, kwargs, end_callback, cancel_callback) for _ in range(num))

    @staticmethod
    def _get_next_coroutine(func: CoroutineFunc, args_iter: Iterator[Any], arg_stars: int = 0) -> Optional[Awaitable]:
        try:
            arg = next(args_iter)
        except StopIteration:
            return
        if arg_stars == 0:
            return func(arg)
        if arg_stars == 1:
            return func(*arg)
        if arg_stars == 2:
            return func(**arg)
        raise ValueError

    async def _map(self, func: CoroutineFunc, args_iter: ArgsT, arg_stars: int = 0, num_tasks: int = 1,
                   end_callback: EndCallbackT = None, cancel_callback: CancelCallbackT = None) -> None:

        if self._all_tasks_known_flag.is_set():
            self._all_tasks_known_flag.clear()
        args_iter = iter(args_iter)

        async def _start_next_coroutine() -> bool:
            cor = self._get_next_coroutine(func, args_iter, arg_stars)
            if cor is None:
                self._all_tasks_known_flag.set()
                return True
            await self._start_task(cor, ignore_closed=True, end_callback=_start_next, cancel_callback=cancel_callback)
            return False

        async def _start_next(task_id: int) -> None:
            await _start_next_coroutine()
            await _execute_function(end_callback, args=(task_id, ))

        for _ in range(num_tasks):
            reached_end = await _start_next_coroutine()
            if reached_end:
                break

    async def map(self, func: CoroutineFunc, args_iter: ArgsT, num_tasks: int = 1,
                  end_callback: EndCallbackT = None, cancel_callback: CancelCallbackT = None) -> None:
        await self._map(func, args_iter, arg_stars=0, num_tasks=num_tasks,
                        end_callback=end_callback, cancel_callback=cancel_callback)

    async def starmap(self, func: CoroutineFunc, args_iter: Iterable[ArgsT], num_tasks: int = 1,
                      end_callback: EndCallbackT = None, cancel_callback: CancelCallbackT = None) -> None:
        await self._map(func, args_iter, arg_stars=1, num_tasks=num_tasks,
                        end_callback=end_callback, cancel_callback=cancel_callback)

    async def doublestarmap(self, func: CoroutineFunc, kwargs_iter: Iterable[KwArgsT], num_tasks: int = 1,
                            end_callback: EndCallbackT = None, cancel_callback: CancelCallbackT = None) -> None:
        await self._map(func, kwargs_iter, arg_stars=2, num_tasks=num_tasks,
                        end_callback=end_callback, cancel_callback=cancel_callback)


class SimpleTaskPool(BaseTaskPool):
    def __init__(self, func: CoroutineFunc, args: ArgsT = (), kwargs: KwArgsT = None,
                 end_callback: EndCallbackT = None, cancel_callback: CancelCallbackT = None,
                 name: str = None) -> None:
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


async def _execute_function(func: Callable, args: ArgsT = (), kwargs: KwArgsT = None) -> None:
    if kwargs is None:
        kwargs = {}
    if callable(func):
        if iscoroutinefunction(func):
            await func(*args, **kwargs)
        else:
            func(*args, **kwargs)
