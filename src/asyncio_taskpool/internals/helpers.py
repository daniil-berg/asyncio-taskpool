"""
Miscellaneous helper functions.

None of these should be considered part of the public API.
"""

from __future__ import annotations

import builtins
from asyncio.coroutines import iscoroutinefunction
from importlib import import_module
from inspect import getdoc
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Iterable,
    Literal,
    Mapping,
    Type,
    TypeVar,
    cast,
    overload,
)
from typing_extensions import ParamSpec

from .constants import PYTHON_BEFORE_39

_P = ParamSpec("_P")
_R = TypeVar("_R")
_T = TypeVar("_T")


@overload
async def execute_optional(
    function: Callable[_P, _R | Awaitable[_R]],
    args: _P.args = (),
    kwargs: _P.kwargs = None,
) -> _R:
    ...


@overload
async def execute_optional(
    function: object,
    args: object = (),
    kwargs: object = None,
) -> None:
    ...


async def execute_optional(
    function: Callable[_P, _R | Awaitable[_R]] | object,
    args: _P.args = (),
    kwargs: _P.kwargs = None,
) -> _R | None:
    """
    Runs `function` with `args` and `kwargs` and returns its output.

    Args:
        function:
            Callable that accepts the provided positional and keyword-arguments.
            If it is a coroutine function, it will be awaited.
            If it is not a callable, nothing is returned.
        *args (optional):
            Positional arguments to pass to `function`.
        **kwargs (optional):
            Keyword-arguments to pass to `function`.

    Returns:
        Whatever `function` returns (possibly after being awaited) or
        `None` if `function` is not callable.
    """
    if not callable(function):
        return None
    if kwargs is None:
        kwargs = {}
    if iscoroutinefunction(function):
        return await cast(Awaitable[_R], function(*args, **kwargs))
    return cast(_R, function(*args, **kwargs))


@overload
def star_function(
    function: Callable[..., _R],
    arg: Mapping[str, object],
    arg_stars: Literal[2],
) -> _R:
    ...


@overload
def star_function(
    function: Callable[..., _R],
    arg: Iterable[object],
    arg_stars: Literal[1],
) -> _R:
    ...


@overload
def star_function(
    function: Callable[..., _R],
    arg: object,
    arg_stars: Literal[0] = 0,
) -> _R:
    ...


def star_function(
    function: Callable[..., _R],
    arg: Any,
    arg_stars: int = 0,
) -> _R:
    """
    Calls `function` passing `arg` to it, optionally unpacking it first.

    Args:
        function:
            Any callable that accepts the provided argument(s).
        arg:
            The single positional argument that `function` expects; in this case
            `arg_stars` should be 0. Or the iterable of positional arguments
            that `function` expects; in this case `arg_stars` should be 1.
            Or the mapping of keyword-arguments that `function` expects; in this
            case `arg_stars` should be 2.
        arg_stars (optional):
            Determines if and how to unpack `arg`. 0 means no unpacking,
            i.e. `arg` is passed into `function` directly as `function(arg)`.
            1 means unpacking to an arbitrary number of positional arguments,
            i.e. as `function(*arg)`. 2 means unpacking to an arbitrary number
            of keyword-arguments, i.e. as `function(**arg)`.

    Returns:
        Whatever `function` returns.

    Raises:
        `ValueError`: `arg_stars` is something other than 0, 1, or 2.
    """
    if arg_stars == 0:
        return function(arg)
    if arg_stars == 1:
        return function(*arg)
    if arg_stars == 2:  # noqa: PLR2004
        return function(**arg)
    raise ValueError(
        f"Invalid argument arg_stars={arg_stars}; must be 0, 1, or 2."
    )


def get_first_doc_line(obj: object) -> str | None:
    """Takes an object and returns the first non-empty line of its docstring."""
    doc = getdoc(obj)
    if doc is None:
        return None
    return doc.strip().split("\n", 1)[0].strip()


async def return_or_exception(
    _function_to_execute: Callable[_P, _R | Awaitable[_R]],
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> _R | Exception:
    """
    Returns the output of a function or the exception thrown during its call.

    Args:
        _function_to_execute:
            Callable that accepts the provided positional and keyword-arguments.
        *args (optional):
            Positional arguments to pass to `_function_to_execute`.
        **kwargs (optional):
            Keyword-arguments to pass to `_function_to_execute`.

    Returns:
        Whatever `_function_to_execute` returns or throws.
        (An exception is not raised, but returned!)
    """
    try:
        if iscoroutinefunction(_function_to_execute):
            return await cast(
                Awaitable[_R], _function_to_execute(*args, **kwargs)
            )
        else:
            return cast(_R, _function_to_execute(*args, **kwargs))
    except Exception as e:
        return e


def resolve_dotted_path(dotted_path: str) -> object:
    """
    Resolves a dotted path to a global object and returns that object.

    Algorithm shamelessly stolen from the `logging.config` module from the
    standard library.
    """
    names = dotted_path.split(".")
    module_name = names.pop(0)
    found = import_module(module_name)
    for name in names:
        try:
            found = getattr(found, name)
        except AttributeError:
            module_name += f".{name}"
            import_module(module_name)
            found = getattr(found, name)
    return found


class ClassMethodWorkaround:
    """Dirty workaround to make `@classmethod` work with properties."""

    def __init__(
        self,
        method_or_property: Callable[..., Any] | property,
    ) -> None:
        if isinstance(method_or_property, property):
            assert (
                method_or_property.fget is not None
            ), "Missing property getter"
            self._getter = method_or_property.fget
        else:
            self._getter = method_or_property

    def __get__(self, obj: _T | None, cls: Type[_T] | None) -> Any:
        if obj is None:
            return self._getter(cls)
        return self._getter(obj)


if not TYPE_CHECKING and PYTHON_BEFORE_39:
    # Starting with Python 3.9, this is thankfully no longer necessary.
    classmethod = ClassMethodWorkaround  # noqa
else:
    classmethod = builtins.classmethod  # noqa
