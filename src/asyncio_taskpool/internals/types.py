"""
Custom type definitions used in various modules.

This module should **not** be considered part of the public API.
"""


from argparse import ArgumentParser, HelpFormatter
from asyncio.streams import StreamReader, StreamWriter
from pathlib import Path
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    IO,
    Iterable,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)
from typing_extensions import NotRequired, Required, TypedDict

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


class _FormatterClass(Protocol):
    def __call__(self, prog: str) -> HelpFormatter:
        ...


class _NameStrRequiredDict(TypedDict):
    name: Required[str]


class _NameStrNotRequiredDict(TypedDict):
    name: NotRequired[str]


class _AddParserKwargsWithoutName(TypedDict, total=False):
    help: Optional[str]
    aliases: Sequence[str]
    prog: Optional[str]
    usage: Optional[str]
    description: Optional[str]
    epilog: Optional[str]
    parents: Sequence[ArgumentParser]
    formatter_class: _FormatterClass
    prefix_chars: str
    fromfile_prefix_chars: Optional[str]
    argument_default: Any
    conflict_handler: str
    add_help: bool
    allow_abbrev: bool
    exit_on_error: bool


class AddParserKwargs(
    _AddParserKwargsWithoutName,
    _NameStrRequiredDict,
    total=False,
):
    """Parameters of `.add_parser` of the `argparse` subparsers action."""


class CommandParserSpecialKwargs(TypedDict, total=False):
    """Added constructor parameters the `CommandParser`."""

    stream: IO[str]
    terminal_width: Optional[int]


class AddSubCommandParserKwargs(
    CommandParserSpecialKwargs,
    _AddParserKwargsWithoutName,
    _NameStrNotRequiredDict,
    total=False,
):
    """Combined parameters to add a `CommandParser` subparser."""
