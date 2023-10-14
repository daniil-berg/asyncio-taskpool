"""
Definition of the :class:`ControlParser` used in a :class:`ControlSession <asyncio_taskpool.control.session.ControlSession>`.

It should not be considered part of the public API.
"""

from __future__ import annotations

import logging
from argparse import (
    SUPPRESS,
    Action,
    ArgumentDefaultsHelpFormatter,
    ArgumentParser,
    ArgumentTypeError,
    HelpFormatter,
)
from ast import literal_eval
from inspect import Parameter, getmembers, isfunction, signature
from shutil import get_terminal_size
from typing import (
    IO,
    Any,
    Callable,
    Container,
    Dict,
    Iterable,
    NoReturn,
    Protocol,
    Set,
    Type,
    TypeVar,
    cast,
    overload,
)

from ..exceptions import HelpRequested, ParserError, SubParsersNotInitialized
from ..internals.constants import CLIENT_INFO, CMD
from ..internals.helpers import get_first_doc_line, resolve_dotted_path
from ..internals.types import AnyCoroutineFunc, ArgsT, CancelCB, EndCB, KwArgsT

__all__ = ["ControlParser"]


log = logging.getLogger(__name__)


FmtCls = TypeVar("FmtCls", bound=HelpFormatter)
ParsersDict = Dict[str, "ControlParser"]

OMIT_PARAMS_DEFAULT = ("self",)

NAME, PROG, HELP, DESCRIPTION = "name", "prog", "help", "description"


class _CanAddControlParser(Protocol):
    def add_parser(self, **kwargs: Any) -> ControlParser:
        ...


class ControlParser(ArgumentParser):
    """
    Subclass of the standard :code:`argparse.ArgumentParser` for pool control.

    Such a parser is not supposed to ever print to stdout/stderr, but instead
    direct all messages to a file-like `StringIO` instance passed to it during
    initialization. Furthermore, it requires defining the width of the terminal,
    to adjust help formatting to the terminal size of a connected client.
    It also offers some convenience methods and makes use of custom exceptions.
    """

    @overload
    @staticmethod
    def help_formatter_factory(
        terminal_width: int,
        base_cls: Type[FmtCls],
    ) -> Type[FmtCls]:
        ...

    @overload
    @staticmethod
    def help_formatter_factory(
        terminal_width: int,
        base_cls: None = None,
    ) -> Type[ArgumentDefaultsHelpFormatter]:
        ...

    @staticmethod
    def help_formatter_factory(
        terminal_width: int,
        base_cls: Type[HelpFormatter] | None = None,
    ) -> Type[HelpFormatter]:
        """
        Constructs and returns a subclass of :class:`argparse.HelpFormatter`.

        The formatter class will have the defined `terminal_width`.

        Although a custom formatter class can be explicitly passed into the
        :class:`ArgumentParser` constructor, this is not as convenient, when
        making use of sub-parsers.

        Args:
            terminal_width:
                Number of terminal columns to which to adjust help formatting.
            base_cls (optional):
                Base class to use for inheritance. By default
                :class:`argparse.ArgumentDefaultsHelpFormatter` is used.

        Returns:
            Subclass of `base_cls` which fixes the constructor's `width`
            keyword-argument to `terminal_width`.
        """
        if base_cls is None:
            base_cls = ArgumentDefaultsHelpFormatter

        class ClientHelpFormatter(base_cls):  # type: ignore[valid-type, misc]
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                kwargs["width"] = terminal_width
                super().__init__(*args, **kwargs)

        return ClientHelpFormatter

    def __init__(
        self,
        stream: IO[str],
        terminal_width: int | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Sets some internal attributes in addition to the base class.

        Args:
            stream:
                A file-like I/O object to use for message output.
            terminal_width (optional):
                The terminal width to use for all message formatting.
                By default the :code:`columns` attribute from
                :func:`shutil.get_terminal_size` is taken.
            **kwargs(optional):
                Passed to the parent class constructor. The exception is the
                `formatter_class` parameter: Even if a class is specified, it
                will always be subclassed in the :meth:`help_formatter_factory`.
        """
        self._stream: IO[str] = stream
        self._terminal_width: int = (
            terminal_width
            if terminal_width is not None
            else get_terminal_size().columns
        )
        kwargs["formatter_class"] = self.help_formatter_factory(
            self._terminal_width, kwargs.get("formatter_class")
        )
        super().__init__(**kwargs)
        self._flags: Set[str] = set()
        self._commands: _CanAddControlParser | None = None

    def add_function_command(
        self,
        function: Callable[..., Any],
        omit_params: Container[str] = OMIT_PARAMS_DEFAULT,
        **subparser_kwargs: Any,
    ) -> ControlParser:
        """
        Takes a function and adds a corresponding (sub-)command to the parser.

        The :meth:`add_subparsers` method must have been called prior to this.

        NOTE: Currently, only a limited spectrum of parameters can be accurately
        converted to parser arguments. This method works correctly with any
        public method of the any task pool class.

        Args:
            function:
                Function to be "converted" to a parser command.
            omit_params (optional):
                Names of function parameters not to add as parser arguments.
            **subparser_kwargs (optional):
                Passed directly to the :meth:`add_parser` method.

        Returns:
            The subparser instance created from the function.
        """
        if self._commands is None:
            raise SubParsersNotInitialized
        subparser_kwargs.setdefault(NAME, function.__name__.replace("_", "-"))
        subparser_kwargs.setdefault(PROG, subparser_kwargs[NAME])
        subparser_kwargs.setdefault(HELP, get_first_doc_line(function))
        subparser_kwargs.setdefault(DESCRIPTION, subparser_kwargs[HELP])
        subparser: ControlParser = self._commands.add_parser(**subparser_kwargs)
        subparser.add_function_args(function, omit_params)
        return subparser

    def add_property_command(
        self,
        prop: property,
        cls_name: str = "",
        **subparser_kwargs: Any,
    ) -> ControlParser:
        """
        Same as the :meth:`add_function_command` method, but for properties.

        Args:
            prop:
                Property to be "converted" to a parser command.
            cls_name (optional):
                Name of the class the property is defined on to appear in the
                command help text.
            **subparser_kwargs (optional):
                Passed directly to the :meth:`add_parser` method.

        Returns:
            The subparser instance created from the property.
        """
        if prop.fget is None:
            raise TypeError("Property must have a getter")  # noqa: TRY003
        if self._commands is None:
            raise SubParsersNotInitialized
        subparser_kwargs.setdefault(NAME, prop.fget.__name__.replace("_", "-"))
        subparser_kwargs.setdefault(PROG, subparser_kwargs[NAME])
        getter_help = get_first_doc_line(prop.fget)
        if prop.fset is None:
            subparser_kwargs.setdefault(HELP, getter_help)
        else:
            subparser_kwargs.setdefault(
                HELP,
                f"Get/set the `{cls_name}.{subparser_kwargs[NAME]}` property",
            )
        subparser_kwargs.setdefault(DESCRIPTION, subparser_kwargs[HELP])
        subparser: ControlParser = self._commands.add_parser(**subparser_kwargs)
        if prop.fset is not None:
            _, param = signature(prop.fset).parameters.values()
            setter_arg_help = (
                f"If provided: {get_first_doc_line(prop.fset)} "
                f"If omitted: {getter_help}"
            )
            subparser.add_function_arg(
                param, nargs="?", default=SUPPRESS, help=setter_arg_help
            )
        return subparser

    def add_class_commands(
        self,
        cls: Type[Any],
        public_only: bool = True,  # noqa: FBT001, FBT002
        omit_members: Container[str] = (),
        member_arg_name: str = CMD,
    ) -> ParsersDict:
        """
        Adds methods/properties of a class as (sub-)commands to the parser.

        The :meth:`add_subparsers` method must have been called prior to this.

        NOTE: Currently, only a limited spectrum of function parameters can be
        accurately converted to parser arguments. This method works correctly
        with any task pool class.

        Args:
            cls:
                Class whose methods/properties are to be "converted" to commands
            public_only (optional):
                If `False`, protected/private members are considered as well.
                `True` by default.
            omit_members (optional):
                Names of functions/properties not to add as parser commands.
            member_arg_name (optional):
                After parsing the arguments, depending on which command was
                invoked by the user, the corresponding method/property will be
                stored as an extra argument in the parsed namespace under this
                attribute name.

        Returns:
            Dictionary mapping member names to the subparsers created from them.
        """
        parsers: ParsersDict = {}
        # TODO: Annotate the following as a TypedDict
        # https://github.com/daniil-berg/asyncio-taskpool/issues/2
        common_kwargs = {
            "stream": self._stream,
            CLIENT_INFO.TERMINAL_WIDTH: self._terminal_width,
        }
        for name, member in getmembers(cls):
            if name in omit_members or (name.startswith("_") and public_only):
                continue
            if isfunction(member):
                subparser = self.add_function_command(member, **common_kwargs)  # type: ignore[arg-type]
            elif isinstance(member, property):
                subparser = self.add_property_command(
                    member, cls.__name__, **common_kwargs
                )
            else:
                continue
            subparser.set_defaults(**{member_arg_name: member})
            parsers[name] = subparser
        return parsers

    def add_subparsers(self, *args: Any, **kwargs: Any) -> Any:
        """Adds the subparsers action as an attribute before returning it."""
        self._commands = cast(
            _CanAddControlParser, super().add_subparsers(*args, **kwargs)
        )
        return self._commands

    def _print_message(self, message: str, *_args: Any, **_kwargs: Any) -> None:
        """Overridden to ensure that messages are sent to the stream buffer."""
        if message:
            self._stream.write(message)

    def exit(self, status: int = 0, message: str | None = None) -> None:  # type: ignore[override]  # noqa: ARG002
        """Overridden to prevent system exit to be invoked."""
        if message:
            self._print_message(message)

    def error(self, message: str) -> NoReturn:
        """Raises the :exc:`ParserError <asyncio_taskpool.exceptions.ParserError>` exception at the end."""
        super().error(message=message)
        raise ParserError

    def print_help(self, file: IO[str] | None = None) -> None:
        """Raises the :exc:`HelpRequested <asyncio_taskpool.exceptions.HelpRequested>` exception at the end."""
        super().print_help(file)
        raise HelpRequested

    def add_function_arg(self, parameter: Parameter, **kwargs: Any) -> Action:
        """
        Adds a :class:`inspect.Parameter` as a corresponding parser argument.

        NOTE: Currently, only a limited spectrum of parameters can be accurately
        converted to a parser argument. This method works correctly with any
        parameter of any public method any task pool class.

        Args:
            parameter:
                :class:`inspect.Parameter` object to be converted to an argument
            **kwargs:
                Passed to the :meth:`add_argument` method of the base class.

        Returns:
            :class:`argparse.Action` returned by :meth:`add_argument`
        """
        if parameter.default is Parameter.empty:
            # A non-optional function parameter should correspond to a
            # positional argument.
            name_or_flags = [parameter.name]
        else:
            flag = None
            long = f'--{parameter.name.replace("_", "-")}'
            # We try to generate a short version (flag) for the argument.
            letter = parameter.name[0]
            if letter not in self._flags:
                flag = f"-{letter}"
                self._flags.add(letter)
            elif letter.upper() not in self._flags:
                flag = f"-{letter.upper()}"
                self._flags.add(letter.upper())
            name_or_flags = [long] if flag is None else [flag, long]
            if parameter.annotation is bool:
                # If we are dealing with a boolean parameter, always use the
                # 'store_true' action. Even if the parameter's default value is
                # `True`, this will make the parser argument's default `False`.
                kwargs.setdefault("action", "store_true")
            else:
                # For now, any other type annotation will implicitly use the
                # default action 'store'. We always set the default value.
                kwargs.setdefault("default", parameter.default)
        if parameter.kind == Parameter.VAR_POSITIONAL:
            # To be able to unpack an arbitrary number of positional arguments.
            kwargs.setdefault("nargs", "*")
        if kwargs.get("action") != "store_true":
            # Set the type from the parameter annotation.
            kwargs.setdefault(
                "type", _get_type_from_annotation(parameter.annotation)
            )
        return self.add_argument(*name_or_flags, **kwargs)

    def add_function_args(
        self,
        function: Callable[..., Any],
        omit: Container[str] = OMIT_PARAMS_DEFAULT,
    ) -> None:
        """
        Takes a function and adds its parameters as arguments to the parser.

        NOTE: Currently, only a limited spectrum of parameters can be accurately
        converted to a parser argument. This method works correctly with any
        public method of any task pool class.

        Args:
            function:
                The function whose parameters are to be converted to parser
                arguments. Its parameters must be properly annotated.
            omit (optional):
                Names of function parameters not to add as parser arguments.
        """
        for param in signature(function).parameters.values():
            if param.name not in omit:
                # TODO: Look into parsing docstrings properly to try and extract
                #       argument help text. For now, the argument help just
                #       shows the type it will be converted to.
                # https://github.com/daniil-berg/asyncio-taskpool/issues/3
                self.add_function_arg(param, help=repr(param.annotation))


def _get_arg_type_wrapper(cls: Type[Any]) -> Callable[[Any], Any]:
    """
    Workaround to avoid a ValueError being raised on suppressed arguments.

    See: https://bugs.python.org/issue36078

    In addition, the type conversion wrapper catches exceptions not handled
    properly by the parser, logs them, and turns them into `ArgumentTypeError`
    exceptions the parser can propagate to the client.

    Returns:
        A wrapper for the constructor of `cls`
    """

    def wrapper(arg: Any) -> Any:
        if arg is SUPPRESS:
            return arg
        try:
            return cls(arg)
        except (ArgumentTypeError, TypeError, ValueError):
            raise  # handled properly by the parser and propagated to the client
        except Exception as e:
            text = (
                f"{e.__class__.__name__} occurred in parser trying to "
                f"convert type: {cls.__name__}({arg!r})"
            )
            log.exception(text)
            raise ArgumentTypeError(text) from e  # propagate to the client

    # Copy the name of the class to maintain useful help messages when
    # incorrect arguments are passed.
    wrapper.__name__ = cls.__name__
    return wrapper


def _get_type_from_annotation(annotation: Any) -> Callable[[Any], Any]:
    """
    Returns a type conversion function based on the `annotation` passed.

    Required to properly convert parsed arguments to the type expected by
    certain pool methods. Each conversion function is wrapped by
    `_get_arg_type_wrapper`.

    `Callable`-type annotations use `resolve_dotted_path` function.
    `Iterable`- or args/kwargs-type annotations use `ast.literal_eval`.
    Others pass unchanged (but still wrapped with `_get_arg_type_wrapper`).
    """
    if any(annotation is t for t in (AnyCoroutineFunc, EndCB, CancelCB)):
        annotation = resolve_dotted_path
    if any(
        annotation is t
        for t in (ArgsT, KwArgsT, Iterable[ArgsT], Iterable[KwArgsT])
    ):
        annotation = literal_eval
    return _get_arg_type_wrapper(annotation)


# ruff: noqa: A003 (Class attribute {} is shadowing a Python builtin)
