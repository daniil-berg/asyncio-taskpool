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
This module contains the the definition of the `ControlParser` class used by a control server.
"""


from argparse import Action, ArgumentParser, ArgumentDefaultsHelpFormatter, HelpFormatter, SUPPRESS
from asyncio.streams import StreamWriter
from inspect import Parameter, getmembers, isfunction, signature
from shutil import get_terminal_size
from typing import Any, Callable, Container, Dict, Set, Type, TypeVar

from ..constants import CLIENT_INFO, CMD, STREAM_WRITER
from ..exceptions import HelpRequested, ParserError
from ..helpers import get_first_doc_line


FmtCls = TypeVar('FmtCls', bound=Type[HelpFormatter])
ParsersDict = Dict[str, 'ControlParser']

OMIT_PARAMS_DEFAULT = ('self', )

NAME, PROG, HELP, DESCRIPTION = 'name', 'prog', 'help', 'description'


class ControlParser(ArgumentParser):
    """
    Subclass of the standard `argparse.ArgumentParser` for remote interaction.

    Such a parser is not supposed to ever print to stdout/stderr, but instead direct all messages to a `StreamWriter`
    instance passed to it during initialization.
    Furthermore, it requires defining the width of the terminal, to adjust help formatting to the terminal size of a
    connected client.
    Finally, it offers some convenience methods and makes use of custom exceptions.
    """

    @staticmethod
    def help_formatter_factory(terminal_width: int, base_cls: FmtCls = None) -> FmtCls:
        """
        Constructs and returns a subclass of `argparse.HelpFormatter` with a fixed terminal width argument.

        Although a custom formatter class can be explicitly passed into the `ArgumentParser` constructor, this is not
        as convenient, when making use of sub-parsers.

        Args:
            terminal_width:
                The number of columns of the terminal to which to adjust help formatting.
            base_cls (optional):
                The base class to use for inheritance. By default `argparse.ArgumentDefaultsHelpFormatter` is used.

        Returns:
            The subclass of `base_cls` which fixes the constructor's `width` keyword-argument to `terminal_width`.
        """
        if base_cls is None:
            base_cls = ArgumentDefaultsHelpFormatter

        class ClientHelpFormatter(base_cls):
            def __init__(self, *args, **kwargs) -> None:
                kwargs['width'] = terminal_width
                super().__init__(*args, **kwargs)
        return ClientHelpFormatter

    def __init__(self, stream_writer: StreamWriter, terminal_width: int = None,
                 **kwargs) -> None:
        """
        Subclass of the `ArgumentParser` geared towards asynchronous interaction with an object "from the outside".

        Allows directing output to a specified writer rather than stdout/stderr and setting terminal width explicitly.

        Args:
            stream_writer:
                The instance of the `asyncio.StreamWriter` to use for message output.
            terminal_width (optional):
                The terminal width to use for all message formatting. Defaults to `shutil.get_terminal_size().columns`.
            **kwargs(optional):
                Passed to the parent class constructor. The exception is the `formatter_class` parameter: Even if a
                class is specified, it will always be subclassed in the `help_formatter_factory`.
                Also, by default, `exit_on_error` is set to `False` (as opposed to how the parent class handles it).
        """
        self._stream_writer: StreamWriter = stream_writer
        self._terminal_width: int = terminal_width if terminal_width is not None else get_terminal_size().columns
        kwargs['formatter_class'] = self.help_formatter_factory(self._terminal_width, kwargs.get('formatter_class'))
        kwargs.setdefault('exit_on_error', False)
        super().__init__(**kwargs)
        self._flags: Set[str] = set()
        self._commands = None

    def add_function_command(self, function: Callable, omit_params: Container[str] = OMIT_PARAMS_DEFAULT,
                             **subparser_kwargs) -> 'ControlParser':
        """
        Takes a function along with its parameters and adds a corresponding (sub-)command to the parser.

        The `add_subparsers` method must have been called prior to this.

        NOTE: Currently, only a limited spectrum of parameters can be accurately converted to a parser argument.
        This method works correctly with any public method of the `SimpleTaskPool` class.

        Args:
            function:
                The reference to the function to be "converted" to a parser command.
            omit_params (optional):
                Names of function parameters not to add as parser arguments.
            **subparser_kwargs (optional):
                Passed directly to the `add_parser` method.

        Returns:
            The subparser instance created from the function.
        """
        subparser_kwargs.setdefault(NAME, function.__name__.replace('_', '-'))
        subparser_kwargs.setdefault(PROG, subparser_kwargs[NAME])
        subparser_kwargs.setdefault(HELP, get_first_doc_line(function))
        subparser_kwargs.setdefault(DESCRIPTION, subparser_kwargs[HELP])
        subparser: ControlParser = self._commands.add_parser(**subparser_kwargs)
        subparser.add_function_args(function, omit_params)
        return subparser

    def add_property_command(self, prop: property, cls_name: str = '', **subparser_kwargs) -> 'ControlParser':
        """
        Same as the `add_function_command` method, but for properties.

        Args:
            prop:
                The reference to the property to be "converted" to a parser command.
            cls_name (optional):
                Name of the class the property is defined on to appear in the command help text.
            **subparser_kwargs (optional):
                Passed directly to the `add_parser` method.

        Returns:
            The subparser instance created from the property.
        """
        subparser_kwargs.setdefault(NAME, prop.fget.__name__.replace('_', '-'))
        subparser_kwargs.setdefault(PROG, subparser_kwargs[NAME])
        getter_help = get_first_doc_line(prop.fget)
        if prop.fset is None:
            subparser_kwargs.setdefault(HELP, getter_help)
        else:
            subparser_kwargs.setdefault(HELP, f"Get/set the `{cls_name}.{subparser_kwargs[NAME]}` property")
        subparser_kwargs.setdefault(DESCRIPTION, subparser_kwargs[HELP])
        subparser: ControlParser = self._commands.add_parser(**subparser_kwargs)
        if prop.fset is not None:
            _, param = signature(prop.fset).parameters.values()
            setter_arg_help = f"If provided: {get_first_doc_line(prop.fset)} If omitted: {getter_help}"
            subparser.add_function_arg(param, nargs='?', default=SUPPRESS, help=setter_arg_help)
        return subparser

    def add_class_commands(self, cls: Type, public_only: bool = True, omit_members: Container[str] = (),
                           member_arg_name: str = CMD) -> ParsersDict:
        """
        Takes a class and adds its methods and properties as (sub-)commands to the parser.

        The `add_subparsers` method must have been called prior to this.

        NOTE: Currently, only a limited spectrum of function parameters can be accurately converted to parser arguments.
        This method works correctly with the `SimpleTaskPool` class.

        Args:
            cls:
                The reference to the class whose methods/properties are to be "converted" to parser commands.
            public_only (optional):
                If `False`, protected and private members are considered as well. `True` by default.
            omit_members (optional):
                Names of functions/properties not to add as parser commands.
            member_arg_name (optional):
                After parsing the arguments, depending on which command was invoked by the user, the corresponding
                method/property will be stored as an extra argument in the parsed namespace under this attribute name.
                Defaults to `constants.CMD`.

        Returns:
            Dictionary mapping class member names to the (sub-)parsers created from them.
        """
        parsers: ParsersDict = {}
        common_kwargs = {STREAM_WRITER: self._stream_writer, CLIENT_INFO.TERMINAL_WIDTH: self._terminal_width}
        for name, member in getmembers(cls):
            if name in omit_members or (name.startswith('_') and public_only):
                continue
            if isfunction(member):
                subparser = self.add_function_command(member, **common_kwargs)
            elif isinstance(member, property):
                subparser = self.add_property_command(member, cls.__name__, **common_kwargs)
            else:
                continue
            subparser.set_defaults(**{member_arg_name: member})
            parsers[name] = subparser
        return parsers

    def add_subparsers(self, *args, **kwargs):
        """Adds the subparsers action as an internal attribute before returning it."""
        self._commands = super().add_subparsers(*args, **kwargs)
        return self._commands

    def _print_message(self, message: str, *args, **kwargs) -> None:
        """This is overridden to ensure that no messages are sent to stdout/stderr, but always to the stream writer."""
        if message:
            self._stream_writer.write(message.encode())

    def exit(self, status: int = 0, message: str = None) -> None:
        """This is overridden to prevent system exit to be invoked."""
        if message:
            self._print_message(message)

    def error(self, message: str) -> None:
        """This just adds the custom `HelpRequested` exception after the parent class' method."""
        super().error(message=message)
        raise ParserError

    def print_help(self, file=None) -> None:
        """This just adds the custom `HelpRequested` exception after the parent class' method."""
        super().print_help(file)
        raise HelpRequested

    def add_function_arg(self, parameter: Parameter, **kwargs) -> Action:
        """
        Takes an `inspect.Parameter` of a function and adds a corresponding argument to the parser.

        NOTE: Currently, only a limited spectrum of parameters can be accurately converted to a parser argument.
        This method works correctly with any parameter of any public method of the `SimpleTaskPool` class.

        Args:
            parameter: The `inspect.Parameter` object to be converted to a parser argument.
            **kwargs: Passed to the `add_argument` method of the base class.

        Returns:
            The `argparse.Action` returned by the `add_argument` method.
        """
        if parameter.default is Parameter.empty:
            # A non-optional function parameter should correspond to a positional argument.
            name_or_flags = [parameter.name]
        else:
            flag = None
            long = f'--{parameter.name.replace("_", "-")}'
            # We try to generate a short version (flag) for the argument.
            letter = parameter.name[0]
            if letter not in self._flags:
                flag = f'-{letter}'
                self._flags.add(letter)
            elif letter.upper() not in self._flags:
                flag = f'-{letter.upper()}'
                self._flags.add(letter.upper())
            name_or_flags = [long] if flag is None else [flag, long]
            if parameter.annotation is bool:
                # If we are dealing with a boolean parameter, always use the 'store_true' action.
                # Even if the parameter's default value is `True`, this will make the parser argument's default `False`.
                kwargs.setdefault('action', 'store_true')
            else:
                # For now, any other type annotation will implicitly use the default action 'store'.
                # In addition, we always set the default value.
                kwargs.setdefault('default', parameter.default)
        if parameter.kind == Parameter.VAR_POSITIONAL:
            # This is to be able to later unpack an arbitrary number of positional arguments.
            kwargs.setdefault('nargs', '*')
        if not kwargs.get('action') == 'store_true':
            # Set the type from the parameter annotation.
            kwargs.setdefault('type', _get_arg_type_wrapper(parameter.annotation))
        return self.add_argument(*name_or_flags, **kwargs)

    def add_function_args(self, function: Callable, omit: Container[str] = OMIT_PARAMS_DEFAULT) -> None:
        """
        Takes a function reference and adds its parameters as arguments to the parser.

        NOTE: Currently, only a limited spectrum of parameters can be accurately converted to a parser argument.
        This method works correctly with any public method of the `SimpleTaskPool` class.

        Args:
            function:
                The function whose parameters are to be converted to parser arguments.
                Its parameters must be properly annotated.
            omit (optional):
                Names of function parameters not to add as parser arguments.
        """
        for param in signature(function).parameters.values():
            if param.name not in omit:
                # TODO: Look into parsing docstrings properly to try and extract argument help text.
                #       For now, the argument help just shows the type it will be converted to.
                self.add_function_arg(param, help=repr(param.annotation))


def _get_arg_type_wrapper(cls: Type) -> Callable[[Any], Any]:
    """
    Returns a wrapper for the constructor of `cls` to avoid a ValueError being raised on suppressed arguments.

    See: https://bugs.python.org/issue36078
    """
    def wrapper(arg: Any) -> Any: return arg if arg is SUPPRESS else cls(arg)
    # Copy the name of the class to maintain useful help messages when incorrect arguments are passed.
    wrapper.__name__ = cls.__name__
    return wrapper
