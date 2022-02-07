from asyncio.coroutines import iscoroutinefunction
from typing import Any, Optional

from .types import T, AnyCallableT, ArgsT, KwArgsT


async def execute_optional(function: AnyCallableT, args: ArgsT = (), kwargs: KwArgsT = None) -> Optional[T]:
    if not callable(function):
        return
    if kwargs is None:
        kwargs = {}
    if iscoroutinefunction(function):
        return await function(*args, **kwargs)
    return function(*args, **kwargs)


def star_function(function: AnyCallableT, arg: Any, arg_stars: int = 0) -> T:
    if arg_stars == 0:
        return function(arg)
    if arg_stars == 1:
        return function(*arg)
    if arg_stars == 2:
        return function(**arg)
    raise ValueError(f"Invalid argument arg_stars={arg_stars}; must be 0, 1, or 2.")
