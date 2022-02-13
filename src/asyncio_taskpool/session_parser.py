from argparse import Action, ArgumentParser, ArgumentDefaultsHelpFormatter, HelpFormatter
from asyncio.streams import StreamWriter
from typing import Type, TypeVar

from .constants import SESSION_PARSER_WRITER, CLIENT_INFO
from .exceptions import HelpRequested


FmtCls = TypeVar('FmtCls', bound=Type[HelpFormatter])
FORMATTER_CLASS = 'formatter_class'
NUM = 'num'


class CommandParser(ArgumentParser):
    @staticmethod
    def help_formatter_factory(terminal_width: int, base_cls: FmtCls = None) -> FmtCls:
        if base_cls is None:
            base_cls = ArgumentDefaultsHelpFormatter

        class ClientHelpFormatter(base_cls):
            def __init__(self, *args, **kwargs) -> None:
                kwargs['width'] = terminal_width
                super().__init__(*args, **kwargs)
        return ClientHelpFormatter

    def __init__(self, *args, **kwargs) -> None:
        parent: CommandParser = kwargs.pop('parent', None)
        self._stream_writer: StreamWriter = parent.stream_writer if parent else kwargs.pop(SESSION_PARSER_WRITER)
        self._terminal_width: int = parent.terminal_width if parent else kwargs.pop(CLIENT_INFO.TERMINAL_WIDTH)
        kwargs[FORMATTER_CLASS] = self.help_formatter_factory(self._terminal_width, kwargs.get(FORMATTER_CLASS))
        kwargs.setdefault('exit_on_error', False)
        super().__init__(*args, **kwargs)

    @property
    def stream_writer(self) -> StreamWriter:
        return self._stream_writer

    @property
    def terminal_width(self) -> int:
        return self._terminal_width

    def _print_message(self, message: str, *args, **kwargs) -> None:
        if message:
            self.stream_writer.write(message.encode())

    def exit(self, status: int = 0, message: str = None) -> None:
        if message:
            self._print_message(message)

    def print_help(self, file=None) -> None:
        super().print_help(file)
        raise HelpRequested

    def add_optional_num_argument(self, *name_or_flags: str, **kwargs) -> Action:
        if not name_or_flags:
            name_or_flags = (NUM, )
        kwargs.setdefault('nargs', '?')
        kwargs.setdefault('default', 1)
        kwargs.setdefault('type', int)
        return self.add_argument(*name_or_flags, **kwargs)
