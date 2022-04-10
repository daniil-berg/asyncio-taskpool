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
Miscellaneous helper functions. None of these should be considered part of the public API.
"""


import builtins
from asyncio.coroutines import iscoroutinefunction
from importlib import import_module
from inspect import getdoc
from typing import Any, Callable, Optional, Type, Union

from .constants import PYTHON_BEFORE_39
from .types import T, AnyCallableT, ArgsT, KwArgsT


async def execute_optional(function: AnyCallableT, args: ArgsT = (), kwargs: KwArgsT = None) -> Optional[T]:
    """
    Runs `function` with `args` and `kwargs` and returns its output.

    Args:
        function:
            Any callable that accepts the provided positional and keyword-arguments.
            If it is a coroutine function, it will be awaited.
            If it is not a callable, nothing is returned.
        *args (optional):
            Positional arguments to pass to `function`.
        **kwargs (optional):
            Keyword-arguments to pass to `function`.

    Returns:
        Whatever `function` returns (possibly after being awaited) or `None` if `function` is not callable.
    """
    if not callable(function):
        return
    if kwargs is None:
        kwargs = {}
    if iscoroutinefunction(function):
        return await function(*args, **kwargs)
    return function(*args, **kwargs)


def star_function(function: AnyCallableT, arg: Any, arg_stars: int = 0) -> T:
    """
    Calls `function` passing `arg` to it, optionally unpacking it first.

    Args:
        function:
            Any callable that accepts the provided argument(s).
        arg:
            The single positional argument that `function` expects; in this case `arg_stars` should be 0.
            Or the iterable of positional arguments that `function` expects; in this case `arg_stars` should be 1.
            Or the mapping of keyword-arguments that `function` expects; in this case `arg_stars` should be 2.
        arg_stars (optional):
            Determines if and how to unpack `arg`.
            0 means no unpacking, i.e. `arg` is passed into `function` directly as `function(arg)`.
            1 means unpacking to an arbitrary number of positional arguments, i.e. as `function(*arg)`.
            2 means unpacking to an arbitrary number of keyword-arguments, i.e. as `function(**arg)`.

    Returns:
        Whatever `function` returns.

    Raises:
        `ValueError`: `arg_stars` is something other than 0, 1, or 2.
    """
    if arg_stars == 0:
        return function(arg)
    if arg_stars == 1:
        return function(*arg)
    if arg_stars == 2:
        return function(**arg)
    raise ValueError(f"Invalid argument arg_stars={arg_stars}; must be 0, 1, or 2.")


def get_first_doc_line(obj: object) -> str:
    """Takes an object and returns the first (non-empty) line of its docstring."""
    return getdoc(obj).strip().split("\n", 1)[0].strip()


async def return_or_exception(_function_to_execute: AnyCallableT, *args, **kwargs) -> Union[T, Exception]:
    """
    Returns the output of a function or the exception thrown during its execution.

    Args:
        _function_to_execute:
            Any callable that accepts the provided positional and keyword-arguments.
        *args (optional):
            Positional arguments to pass to `_function_to_execute`.
        **kwargs (optional):
            Keyword-arguments to pass to `_function_to_execute`.

    Returns:
        Whatever `_function_to_execute` returns or throws. (An exception is not raised, but returned!)
    """
    try:
        if iscoroutinefunction(_function_to_execute):
            return await _function_to_execute(*args, **kwargs)
        else:
            return _function_to_execute(*args, **kwargs)
    except Exception as e:
        return e


def resolve_dotted_path(dotted_path: str) -> object:
    """
    Resolves a dotted path to a global object and returns that object.

    Algorithm shamelessly stolen from the `logging.config` module from the standard library.
    """
    names = dotted_path.split('.')
    module_name = names.pop(0)
    found = import_module(module_name)
    for name in names:
        try:
            found = getattr(found, name)
        except AttributeError:
            module_name += f'.{name}'
            import_module(module_name)
            found = getattr(found, name)
    return found


class ClassMethodWorkaround:
    """Dirty workaround to make the `@classmethod` decorator work with properties."""

    def __init__(self, method_or_property: Union[Callable, property]) -> None:
        if isinstance(method_or_property, property):
            self._getter = method_or_property.fget
        else:
            self._getter = method_or_property

    def __get__(self, obj: Union[T, None], cls: Union[Type[T], None]) -> Any:
        if obj is None:
            return self._getter(cls)
        return self._getter(obj)


# Starting with Python 3.9, this is thankfully no longer necessary.
if PYTHON_BEFORE_39:
    classmethod = ClassMethodWorkaround
else:
    classmethod = builtins.classmethod
