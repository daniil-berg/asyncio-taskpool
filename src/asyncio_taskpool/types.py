from typing import Callable, Awaitable, Any


CoroutineFunc = Callable[[...], Awaitable[Any]]
FinalCallbackT = Callable
CancelCallbackT = Callable
