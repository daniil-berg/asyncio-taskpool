import logging
from asyncio import gather
from asyncio.coroutines import iscoroutinefunction
from asyncio.exceptions import CancelledError
from asyncio.locks import Event
from asyncio.tasks import Task, create_task
from math import inf
from typing import Any, Awaitable, Callable, Dict, Iterable, Iterator, List, Optional, Tuple

from . import exceptions
from .types import ArgsT, KwArgsT, CoroutineFunc, FinalCallbackT, CancelCallbackT


log = logging.getLogger(__name__)


class BaseTaskPool:
    _pools: List['BaseTaskPool'] = []

    @classmethod
    def _add_pool(cls, pool: 'BaseTaskPool') -> int:
        cls._pools.append(pool)
        return len(cls._pools) - 1

    # TODO: Make use of `max_running`
    def __init__(self, max_running: int = inf, name: str = None) -> None:
        self._max_running: int = max_running
        self._open: bool = True
        self._counter: int = 0
        self._running: Dict[int, Task] = {}
        self._cancelled: Dict[int, Task] = {}
        self._ended: Dict[int, Task] = {}
        self._idx: int = self._add_pool(self)
        self._name: str = name
        self._all_tasks_known_flag: Event = Event()
        self._all_tasks_known_flag.set()
        self._more_allowed_flag: Event = Event()
        self._check_more_allowed()
        log.debug("%s initialized", str(self))

    def __str__(self) -> str:
        return f'{self.__class__.__name__}-{self._name or self._idx}'

    @property
    def max_running(self) -> int:
        return self._max_running

    @property
    def is_open(self) -> bool:
        return self._open

    @property
    def num_running(self) -> int:
        return len(self._running)

    @property
    def num_cancelled(self) -> int:
        return len(self._cancelled)

    @property
    def num_ended(self) -> int:
        return len(self._ended)

    @property
    def num_finished(self) -> int:
        return self.num_ended - self.num_cancelled

    @property
    def is_full(self) -> bool:
        return not self._more_allowed_flag.is_set()

    def _check_more_allowed(self) -> None:
        if self.is_full and self.num_running < self.max_running:
            self._more_allowed_flag.set()
        elif not self.is_full and self.num_running >= self.max_running:
            self._more_allowed_flag.clear()

    def _task_name(self, task_id: int) -> str:
        return f'{self}_Task-{task_id}'

    async def _cancel_task(self, task_id: int, custom_callback: CancelCallbackT = None) -> None:
        log.debug("Cancelling %s ...", self._task_name(task_id))
        task = self._running.pop(task_id)
        assert task is not None
        self._cancelled[task_id] = task
        await _execute_function(custom_callback, args=(task_id, ))
        log.debug("Cancelled %s", self._task_name(task_id))

    async def _end_task(self, task_id: int, custom_callback: FinalCallbackT = None) -> None:
        task = self._running.pop(task_id, None)
        if task is None:
            task = self._cancelled[task_id]
        self._ended[task_id] = task
        await _execute_function(custom_callback, args=(task_id, ))
        self._check_more_allowed()
        log.info("Ended %s", self._task_name(task_id))

    async def _task_wrapper(self, awaitable: Awaitable, task_id: int, final_callback: FinalCallbackT = None,
                            cancel_callback: CancelCallbackT = None) -> Any:
        log.info("Started %s", self._task_name(task_id))
        try:
            return await awaitable
        except CancelledError:
            await self._cancel_task(task_id, custom_callback=cancel_callback)
        finally:
            await self._end_task(task_id, custom_callback=final_callback)

    def _start_task(self, awaitable: Awaitable, ignore_closed: bool = False, final_callback: FinalCallbackT = None,
                    cancel_callback: CancelCallbackT = None) -> int:
        if not (self._open or ignore_closed):
            raise exceptions.PoolIsClosed("Cannot start new tasks")
        task_id = self._counter
        self._counter += 1
        self._running[task_id] = create_task(
            self._task_wrapper(awaitable, task_id, final_callback, cancel_callback),
            name=self._task_name(task_id)
        )
        self._check_more_allowed()
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

    def close(self) -> None:
        self._open = False
        log.info("%s is closed!", str(self))

    async def gather(self, return_exceptions: bool = False):
        if self._open:
            raise exceptions.PoolStillOpen("Pool must be closed, before tasks can be gathered")
        await self._all_tasks_known_flag.wait()
        results = await gather(*self._running.values(), *self._ended.values(), return_exceptions=return_exceptions)
        self._running = self._cancelled = self._ended = {}
        return results


class TaskPool(BaseTaskPool):
    def _apply_one(self, func: CoroutineFunc, args: ArgsT = (), kwargs: KwArgsT = None,
                   final_callback: FinalCallbackT = None, cancel_callback: CancelCallbackT = None) -> int:
        if kwargs is None:
            kwargs = {}
        return self._start_task(func(*args, **kwargs), final_callback=final_callback, cancel_callback=cancel_callback)

    def apply(self, func: CoroutineFunc, args: ArgsT = (), kwargs: KwArgsT = None, num: int = 1,
              final_callback: FinalCallbackT = None, cancel_callback: CancelCallbackT = None) -> Tuple[int]:
        return tuple(self._apply_one(func, args, kwargs, final_callback, cancel_callback) for _ in range(num))

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

    def _map(self, func: CoroutineFunc, args_iter: ArgsT, arg_stars: int = 0, num_tasks: int = 1,
             final_callback: FinalCallbackT = None, cancel_callback: CancelCallbackT = None) -> None:

        if self._all_tasks_known_flag.is_set():
            self._all_tasks_known_flag.clear()
        args_iter = iter(args_iter)

        def _start_next_coroutine() -> bool:
            cor = self._get_next_coroutine(func, args_iter, arg_stars)
            if cor is None:
                self._all_tasks_known_flag.set()
                return True
            self._start_task(cor, ignore_closed=True, final_callback=_start_next, cancel_callback=cancel_callback)
            return False

        async def _start_next(task_id: int) -> None:
            await _execute_function(final_callback, args=(task_id, ))
            _start_next_coroutine()

        for _ in range(num_tasks):
            reached_end = _start_next_coroutine()
            if reached_end:
                break

    def map(self, func: CoroutineFunc, args_iter: ArgsT, num_tasks: int = 1,
            final_callback: FinalCallbackT = None, cancel_callback: CancelCallbackT = None) -> None:
        self._map(func, args_iter, arg_stars=0, num_tasks=num_tasks,
                  final_callback=final_callback, cancel_callback=cancel_callback)

    def starmap(self, func: CoroutineFunc, args_iter: Iterable[ArgsT], num_tasks: int = 1,
                final_callback: FinalCallbackT = None, cancel_callback: CancelCallbackT = None) -> None:
        self._map(func, args_iter, arg_stars=1, num_tasks=num_tasks,
                  final_callback=final_callback, cancel_callback=cancel_callback)

    def doublestarmap(self, func: CoroutineFunc, kwargs_iter: Iterable[KwArgsT], num_tasks: int = 1,
                      final_callback: FinalCallbackT = None, cancel_callback: CancelCallbackT = None) -> None:
        self._map(func, kwargs_iter, arg_stars=2, num_tasks=num_tasks,
                  final_callback=final_callback, cancel_callback=cancel_callback)


class SimpleTaskPool(BaseTaskPool):
    def __init__(self, func: CoroutineFunc, args: ArgsT = (), kwargs: KwArgsT = None,
                 final_callback: FinalCallbackT = None, cancel_callback: CancelCallbackT = None,
                 name: str = None) -> None:
        self._func: CoroutineFunc = func
        self._args: ArgsT = args
        self._kwargs: KwArgsT = kwargs if kwargs is not None else {}
        self._final_callback: FinalCallbackT = final_callback
        self._cancel_callback: CancelCallbackT = cancel_callback
        super().__init__(name=name)

    @property
    def func_name(self) -> str:
        return self._func.__name__

    @property
    def size(self) -> int:
        return self.num_running

    def _start_one(self) -> int:
        return self._start_task(self._func(*self._args, **self._kwargs),
                                final_callback=self._final_callback, cancel_callback=self._cancel_callback)

    def start(self, num: int = 1) -> List[int]:
        return [self._start_one() for _ in range(num)]

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
