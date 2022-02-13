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
Miscellaneous helper functions.
"""


from asyncio.coroutines import iscoroutinefunction
from asyncio.queues import Queue
from inspect import getdoc
from typing import Any, Optional, Union

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


async def join_queue(q: Queue) -> None:
    await q.join()


def tasks_str(num: int) -> str:
    return "tasks" if num != 1 else "task"


def get_first_doc_line(obj: object) -> str:
    return getdoc(obj).strip().split("\n", 1)[0].strip()


async def return_or_exception(_function_to_execute: AnyCallableT, *args, **kwargs) -> Union[T, Exception]:
    try:
        if iscoroutinefunction(_function_to_execute):
            return await _function_to_execute(*args, **kwargs)
        else:
            return _function_to_execute(*args, **kwargs)
    except Exception as e:
        return e
