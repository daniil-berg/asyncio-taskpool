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
This module contains the definition of an `asyncio.Queue` subclass.
"""


from asyncio.queues import Queue as _Queue
from typing import Any


class Queue(_Queue):
    """This just adds a little syntactic sugar to the `asyncio.Queue`."""

    def item_processed(self) -> None:
        """
        Does exactly the same as `task_done()`.

        This method exists because `task_done` is an atrocious name for the method. It communicates the wrong thing,
        invites confusion, and immensely reduces readability (in the context of this library). And readability counts.
        """
        self.task_done()

    async def __aenter__(self) -> Any:
        """
        Implements an asynchronous context manager for the queue.

        Upon entering `get()` is awaited and subsequently whatever came out of the queue is returned.
        It allows writing code this way:
        >>> queue = Queue()
        >>> ...
        >>> async with queue as item:
        >>>     ...
        """
        return await self.get()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Implements an asynchronous context manager for the queue.

        Upon exiting `item_processed()` is called. This is why this context manager may not always be what you want,
        but in some situations it makes the code much cleaner.
        """
        self.item_processed()
