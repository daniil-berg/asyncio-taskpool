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
This module contains the the definition of the `CommandParser` class used in a control server session.
"""


from argparse import Action, ArgumentParser, ArgumentDefaultsHelpFormatter, HelpFormatter
from asyncio.streams import StreamWriter
from typing import Type, TypeVar

from .constants import SESSION_WRITER, CLIENT_INFO
from .exceptions import HelpRequested


FmtCls = TypeVar('FmtCls', bound=Type[HelpFormatter])
FORMATTER_CLASS = 'formatter_class'
NUM = 'num'


class CommandParser(ArgumentParser):
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

    def __init__(self, parent: 'CommandParser' = None, **kwargs) -> None:
        """
        Sets additional internal attributes depending on whether a parent-parser was defined.

        The `help_formatter_factory` is called and the returned class is mapped to the `FORMATTER_CLASS` keyword.
        By default, `exit_on_error` is set to `False` (as opposed to how the parent class handles it).

        Args:
            parent (optional):
                An instance of the same class. Intended to be passed as a keyword-argument into the `add_parser` method
                of the subparsers action returned by the `ArgumentParser.add_subparsers` method. If this is present,
                the `SESSION_WRITER` and `CLIENT_INFO.TERMINAL_WIDTH` keywords must not be present in `kwargs`.
            **kwargs(optional):
                In addition to the regular `ArgumentParser` constructor parameters, this method expects the instance of
                the `StreamWriter` as well as the terminal width both to be passed explicitly, if the `parent` argument
                is empty.
        """
        self._session_writer: StreamWriter = parent.session_writer if parent else kwargs.pop(SESSION_WRITER)
        self._terminal_width: int = parent.terminal_width if parent else kwargs.pop(CLIENT_INFO.TERMINAL_WIDTH)
        kwargs[FORMATTER_CLASS] = self.help_formatter_factory(self._terminal_width, kwargs.get(FORMATTER_CLASS))
        kwargs.setdefault('exit_on_error', False)
        super().__init__(**kwargs)

    @property
    def session_writer(self) -> StreamWriter:
        """Returns the predefined stream writer object of the control session."""
        return self._session_writer

    @property
    def terminal_width(self) -> int:
        """Returns the predefined terminal width."""
        return self._terminal_width

    def _print_message(self, message: str, *args, **kwargs) -> None:
        """This is overridden to ensure that no messages are sent to stdout/stderr, but always to the stream writer."""
        if message:
            self._session_writer.write(message.encode())

    def exit(self, status: int = 0, message: str = None) -> None:
        """This is overridden to prevent system exit to be invoked."""
        if message:
            self._print_message(message)

    def print_help(self, file=None) -> None:
        """This just adds the custom `HelpRequested` exception after the parent class' method."""
        super().print_help(file)
        raise HelpRequested

    def add_optional_num_argument(self, *name_or_flags: str, **kwargs) -> Action:
        """Convenience method for `add_argument` setting the name, `nargs`, `default`, and `type`, unless specified."""
        if not name_or_flags:
            name_or_flags = (NUM, )
        kwargs.setdefault('nargs', '?')
        kwargs.setdefault('default', 1)
        kwargs.setdefault('type', int)
        return self.add_argument(*name_or_flags, **kwargs)
