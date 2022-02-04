from asyncio.streams import StreamReader, StreamWriter
from typing import Tuple, Callable, Awaitable, Union, Any


CoroutineFunc = Callable[[...], Awaitable[Any]]
FinalCallbackT = Callable
CancelCallbackT = Callable

ClientConnT = Union[Tuple[StreamReader, StreamWriter], Tuple[None, None]]
