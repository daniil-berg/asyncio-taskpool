import logging
from asyncio import gather
from asyncio.tasks import Task
from typing import Mapping, List, Iterable, Any

from .types import CoroutineFunc, FinalCallbackT, CancelCallbackT
from .task import start_task


log = logging.getLogger(__name__)


class TaskPool:
    _pools: List['TaskPool'] = []

    @classmethod
    def _add_pool(cls, pool: 'TaskPool') -> int:
        cls._pools.append(pool)
        return len(cls._pools) - 1

    def __init__(self, func: CoroutineFunc, args: Iterable[Any] = (), kwargs: Mapping[str, Any] = None,
                 final_callback: FinalCallbackT = None, cancel_callback: CancelCallbackT = None,
                 name: str = None) -> None:
        self._func: CoroutineFunc = func
        self._args: Iterable[Any] = args
        self._kwargs: Mapping[str, Any] = kwargs if kwargs is not None else {}
        self._final_callback: FinalCallbackT = final_callback
        self._cancel_callback: CancelCallbackT = cancel_callback
        self._tasks: List[Task] = []
        self._cancelled: List[Task] = []
        self._idx: int = self._add_pool(self)
        self._name: str = name
        log.debug("%s initialized", repr(self))

    @property
    def func_name(self) -> str:
        return self._func.__name__

    @property
    def size(self) -> int:
        return len(self._tasks)

    def __str__(self) -> str:
        return f'{self.__class__.__name__}-{self._name or self._idx}'

    def __repr__(self) -> str:
        return f'<{self} func={self.func_name}>'

    def _task_name(self, i: int) -> str:
        return f'{self.func_name}_pool_task_{i}'

    def _start_one(self) -> None:
        self._tasks.append(start_task(self._func(*self._args, **self._kwargs), self._task_name(self.size),
                                      final_callback=self._final_callback, cancel_callback=self._cancel_callback))

    def start(self, num: int = 1) -> None:
        for _ in range(num):
            self._start_one()

    def stop(self, num: int = 1) -> int:
        for i in range(num):
            try:
                task = self._tasks.pop()
            except IndexError:
                num = i
                break
            task.cancel()
            self._cancelled.append(task)
        return num

    def stop_all(self) -> int:
        return self.stop(self.size)

    async def gather(self, return_exceptions: bool = False):
        results = await gather(*self._tasks, *self._cancelled, return_exceptions=return_exceptions)
        self._tasks = self._cancelled = []
        return results
