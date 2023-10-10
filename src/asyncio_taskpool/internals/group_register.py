"""
Definition of :class:`TaskGroupRegister`.

It should not be considered part of the public API.
"""

from __future__ import annotations

from asyncio.locks import Lock
from types import TracebackType
from typing import Iterator, MutableSet, Set, Type, TypeVar

_E = TypeVar("_E", bound=BaseException)


class TaskGroupRegister(MutableSet[int]):
    """
    Combines the interface of a regular `set` with that of the `asyncio.Lock`.

    Serves simultaneously as a container of IDs of tasks that belong to the same group, and as a mechanism for
    preventing race conditions within a task group. The lock should be acquired before cancelling the entire group of
    tasks, as well as before starting a task within the group.
    """

    def __init__(self, *task_ids: int) -> None:
        self._ids: Set[int] = set(task_ids)
        self._lock = Lock()

    def __contains__(self, task_id: object) -> bool:
        """Abstract method for the `MutableSet` base class."""
        return task_id in self._ids

    def __iter__(self) -> Iterator[int]:
        """Abstract method for the `MutableSet` base class."""
        return iter(self._ids)

    def __len__(self) -> int:
        """Abstract method for the `MutableSet` base class."""
        return len(self._ids)

    def add(self, task_id: int) -> None:
        """Abstract method for the `MutableSet` base class."""
        self._ids.add(task_id)

    def discard(self, task_id: int) -> None:
        """Abstract method for the `MutableSet` base class."""
        self._ids.discard(task_id)

    async def acquire(self) -> bool:
        """Wrapper around the lock's `acquire()` method."""
        return await self._lock.acquire()

    def release(self) -> None:
        """Wrapper around the lock's `release()` method."""
        self._lock.release()

    async def __aenter__(self) -> None:
        """Provides the asynchronous context manager syntax `async with ... :` when using the lock."""
        await self._lock.acquire()
        return None

    async def __aexit__(
        self,
        exc_type: Type[_E] | None,
        exc_val: _E | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Provides the asynchronous context manager syntax `async with ... :` when using the lock."""
        self._lock.release()
