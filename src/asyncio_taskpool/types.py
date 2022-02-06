from asyncio.streams import StreamReader, StreamWriter
from typing import Any, Awaitable, Callable, Iterable, Mapping, Tuple, Union


ArgsT = Iterable[Any]
KwArgsT = Mapping[str, Any]
CoroutineFunc = Callable[[...], Awaitable[Any]]
EndCallbackT = Callable
CancelCallbackT = Callable

ClientConnT = Union[Tuple[StreamReader, StreamWriter], Tuple[None, None]]
