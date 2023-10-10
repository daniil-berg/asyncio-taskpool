"""
Definition of an :code:`asyncio.Queue` subclass with some small additions.
"""

from __future__ import annotations
import sys
from asyncio.queues import Queue as _Queue
from types import TracebackType
from typing import TYPE_CHECKING, Any, Type, TypeVar


__all__ = ['Queue']

_E = TypeVar("_E", bound=BaseException)
_T = TypeVar("_T")

# Hack around the fact that in Python 3.8 `asyncio.Queue` is not generic:
if not TYPE_CHECKING and sys.version_info < (3, 9):
    _Queue.__class_getitem__ = lambda _: _Queue


class Queue(_Queue[_T]):
    """
    Adds a little syntactic sugar to the :code:`asyncio.Queue`.

    Allows being used as an async context manager awaiting `get` upon entering the context and calling
    :meth:`item_processed` upon exiting it.
    """

    def item_processed(self) -> None:
        """
        Does exactly the same as :meth:`asyncio.Queue.task_done`.

        This method exists because `task_done` is an atrocious name for the method. It communicates the wrong thing,
        invites confusion, and immensely reduces readability (in the context of this library). And readability counts.
        """
        self.task_done()

    async def __aenter__(self) -> Any:
        """
        Implements an asynchronous context manager for the queue.

        Upon entering :meth:`get` is awaited and subsequently whatever came out of the queue is returned.
        It allows writing code this way:
        >>> queue = Queue()
        >>> ...
        >>> async with queue as item:
        >>>     ...
        """
        return await self.get()

    async def __aexit__(self, exc_type: Type[_E] | None, exc_val: _E | None, exc_tb: TracebackType | None) -> None:
        """
        Implements an asynchronous context manager for the queue.

        Upon exiting :meth:`item_processed` is called. This is why this context manager may not always be what you want,
        but in some situations it makes the code much cleaner.
        """
        self.item_processed()
