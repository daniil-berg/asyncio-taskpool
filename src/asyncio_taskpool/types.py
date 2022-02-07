from asyncio.streams import StreamReader, StreamWriter
from typing import Any, Awaitable, Callable, Iterable, Mapping, Tuple, TypeVar, Union


T = TypeVar('T')

ArgsT = Iterable[Any]
KwArgsT = Mapping[str, Any]

AnyCallableT = Callable[[...], Union[Awaitable[T], T]]
CoroutineFunc = Callable[[...], Awaitable[Any]]

EndCallbackT = Callable
CancelCallbackT = Callable

ClientConnT = Union[Tuple[StreamReader, StreamWriter], Tuple[None, None]]
