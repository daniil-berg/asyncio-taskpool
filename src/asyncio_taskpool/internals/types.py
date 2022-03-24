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
Custom type definitions used in various modules.

This module should **not** be considered part of the public API.
"""


from asyncio.streams import StreamReader, StreamWriter
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Mapping, Tuple, TypeVar, Union


T = TypeVar('T')

ArgsT = Iterable[Any]
KwArgsT = Mapping[str, Any]

AnyCallableT = Callable[[...], Union[T, Awaitable[T]]]
CoroutineFunc = Callable[[...], Awaitable[Any]]

EndCB = Callable
CancelCB = Callable

ConnectedCallbackT = Callable[[StreamReader, StreamWriter], Awaitable[None]]
ClientConnT = Union[Tuple[StreamReader, StreamWriter], Tuple[None, None]]

PathT = Union[Path, str]
