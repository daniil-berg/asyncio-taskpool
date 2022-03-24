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
Definition of :class:`TaskGroupRegister`.

It should not be considered part of the public API.
"""


from asyncio.locks import Lock
from collections.abc import MutableSet
from typing import Iterator, Set


class TaskGroupRegister(MutableSet):
    """
    Combines the interface of a regular `set` with that of the `asyncio.Lock`.

    Serves simultaneously as a container of IDs of tasks that belong to the same group, and as a mechanism for
    preventing race conditions within a task group. The lock should be acquired before cancelling the entire group of
    tasks, as well as before starting a task within the group.
    """

    def __init__(self, *task_ids: int) -> None:
        self._ids: Set[int] = set(task_ids)
        self._lock = Lock()

    def __contains__(self, task_id: int) -> bool:
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

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """Provides the asynchronous context manager syntax `async with ... :` when using the lock."""
        self._lock.release()
