"""
Custom type definitions used in various modules.

This module should **not** be considered part of the public API.
"""


from asyncio.streams import StreamReader, StreamWriter
from pathlib import Path
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Iterable,
    Mapping,
    Tuple,
    TypeVar,
    Union,
)

T = TypeVar("T")

ArgsT = Iterable[Any]
KwArgsT = Mapping[str, Any]

AnyCallableT = Callable[..., Union[T, Awaitable[T]]]
AnyCoroutine = Coroutine[Any, Any, Any]
AnyCoroutineFunc = Callable[..., AnyCoroutine]

EndCB = Callable[[int], Any]
CancelCB = Callable[[int], Any]

ConnectedCallbackT = Callable[[StreamReader, StreamWriter], Awaitable[None]]
ClientConnT = Union[Tuple[StreamReader, StreamWriter], Tuple[None, None]]

PathT = Union[Path, str]
