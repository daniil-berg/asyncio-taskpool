import logging
import json
from argparse import ArgumentError, ArgumentParser, HelpFormatter, Namespace
from asyncio.streams import StreamReader, StreamWriter
from typing import Callable, Optional, Type, Union, TYPE_CHECKING

from . import constants
from .exceptions import HelpRequested
from .helpers import get_first_doc_line, return_or_exception, tasks_str
from .pool import TaskPool, SimpleTaskPool

if TYPE_CHECKING:
    from .server import ControlServer


log = logging.getLogger(__name__)

NUM = 'num'
WIDTH = 'width'


class CommandParser(ArgumentParser):
    @staticmethod
    def help_formatter_factory(terminal_width: int) -> Type[HelpFormatter]:
        class ClientHelpFormatter(HelpFormatter):
            def __init__(self, *args, **kwargs) -> None:
                kwargs[WIDTH] = terminal_width
                super().__init__(*args, **kwargs)
        return ClientHelpFormatter

    def __init__(self, *args, **kwargs) -> None:
        parent: CommandParser = kwargs.pop('parent', None)
        self._stream_writer: StreamWriter = parent.stream_writer if parent else kwargs.pop('writer')
        self._terminal_width: int = parent.terminal_width if parent else kwargs.pop(WIDTH)
        kwargs.setdefault('formatter_class', self.help_formatter_factory(self._terminal_width))
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


class ControlSession:
    def __init__(self, server: 'ControlServer', reader: StreamReader, writer: StreamWriter) -> None:
        self._control_server: 'ControlServer' = server
        self._pool: Union[TaskPool, SimpleTaskPool] = server.pool
        self._client_class_name = server.client_class_name
        self._reader: StreamReader = reader
        self._writer: StreamWriter = writer
        self._parser: Optional[CommandParser] = None

    def _add_base_commands(self):
        subparsers = self._parser.add_subparsers(title="Commands", dest=constants.CMD)
        subparsers.add_parser(
            constants.CMD_NAME,
            prog=constants.CMD_NAME,
            help=get_first_doc_line(self._pool.__class__.__str__),
            parent=self._parser,
        )
        subparser_pool_size = subparsers.add_parser(
            constants.CMD_POOL_SIZE,
            prog=constants.CMD_POOL_SIZE,
            help="Get/set the maximum number of tasks in the pool",
            parent=self._parser,
        )
        subparser_pool_size.add_argument(
            NUM,
            nargs='?',
            help=f"If passed a number: {get_first_doc_line(self._pool.__class__.pool_size.fset)} "
                 f"If omitted: {get_first_doc_line(self._pool.__class__.pool_size.fget)}"
        )
        subparsers.add_parser(
            constants.CMD_NUM_RUNNING,
            help=get_first_doc_line(self._pool.__class__.num_running.fget),
            parent=self._parser,
        )
        return subparsers

    def _add_simple_commands(self):
        subparsers = self._add_base_commands()
        subparser = subparsers.add_parser(
            constants.CMD_START,
            prog=constants.CMD_START,
            help=get_first_doc_line(self._pool.__class__.start),
            parent=self._parser,
        )
        subparser.add_argument(
            NUM,
            nargs='?',
            type=int,
            default=1,
            help="Number of tasks to start. Defaults to 1."
        )
        subparser = subparsers.add_parser(
            constants.CMD_STOP,
            prog=constants.CMD_STOP,
            help=get_first_doc_line(self._pool.__class__.stop),
            parent=self._parser,
        )
        subparser.add_argument(
            NUM,
            nargs='?',
            type=int,
            default=1,
            help="Number of tasks to stop. Defaults to 1."
        )
        subparsers.add_parser(
            constants.CMD_STOP_ALL,
            prog=constants.CMD_STOP_ALL,
            help=get_first_doc_line(self._pool.__class__.stop_all),
            parent=self._parser,
        )
        subparsers.add_parser(
            constants.CMD_FUNC_NAME,
            prog=constants.CMD_FUNC_NAME,
            help=get_first_doc_line(self._pool.__class__.func_name.fget),
            parent=self._parser,
        )

    def _init_parser(self, client_terminal_width: int) -> None:
        self._parser = CommandParser(prog='', writer=self._writer, width=client_terminal_width)
        if isinstance(self._pool, TaskPool):
            pass  # TODO
        elif isinstance(self._pool, SimpleTaskPool):
            self._add_simple_commands()

    async def client_handshake(self) -> None:
        client_info = json.loads((await self._reader.read(constants.MSG_BYTES)).decode().strip())
        log.debug("%s connected", self._client_class_name)
        self._init_parser(client_info[WIDTH])
        self._writer.write(str(self._pool).encode())
        await self._writer.drain()

    async def _write_function_output(self, func: Callable, *args, **kwargs) -> None:
        output = await return_or_exception(func, *args, **kwargs)
        self._writer.write(b"ok" if output is None else str(output).encode())

    async def _cmd_name(self, **_kwargs) -> None:
        log.debug("%s requests task pool name", self._client_class_name)
        await self._write_function_output(self._pool.__class__.__str__, self._pool)

    async def _cmd_pool_size(self, **kwargs) -> None:
        num = kwargs.get(NUM)
        if num is None:
            log.debug("%s requests pool size", self._client_class_name)
            await self._write_function_output(self._pool.__class__.pool_size.fget, self._pool)
        else:
            log.debug("%s requests setting pool size to %s", self._client_class_name, num)
            await self._write_function_output(self._pool.__class__.pool_size.fset, self._pool, int(num))

    async def _cmd_num_running(self, **_kwargs) -> None:
        log.debug("%s requests number of running tasks", self._client_class_name)
        await self._write_function_output(self._pool.__class__.num_running.fget, self._pool)

    async def _cmd_start(self, **kwargs) -> None:
        num = kwargs[NUM]
        log.debug("%s requests starting %s %s", self._client_class_name, num, tasks_str(num))
        await self._write_function_output(self._pool.start, num)

    async def _cmd_stop(self, **kwargs) -> None:
        num = kwargs[NUM]
        log.debug("%s requests stopping %s %s", self._client_class_name, num, tasks_str(num))
        await self._write_function_output(self._pool.stop, num)

    async def _cmd_stop_all(self, **_kwargs) -> None:
        log.debug("%s requests stopping all tasks", self._client_class_name)
        await self._write_function_output(self._pool.stop_all)

    async def _cmd_func_name(self, **_kwargs) -> None:
        log.debug("%s requests pool function name", self._client_class_name)
        await self._write_function_output(self._pool.__class__.func_name.fget, self._pool)

    async def _execute_command(self, args: Namespace) -> None:
        args = vars(args)
        cmd: str = args.pop(constants.CMD, None)
        if cmd is not None:
            method = getattr(self, f'_cmd_{cmd.replace("-", "_")}')
            await method(**args)

    async def _parse_command(self, msg: str) -> None:
        try:
            args, argv = self._parser.parse_known_args(msg.split(' '))
        except ArgumentError as e:
            self._writer.write(str(e).encode())
            return
        except HelpRequested:
            return
        if argv:
            log.debug("%s sent unknown arguments: %s", self._client_class_name, msg)
            self._writer.write(b"Invalid command!")
            return
        await self._execute_command(args)

    async def listen(self) -> None:
        while self._control_server.is_serving():
            msg = (await self._reader.read(constants.MSG_BYTES)).decode().strip()
            if not msg:
                log.debug("%s disconnected", self._client_class_name)
                break
            await self._parse_command(msg)
            await self._writer.drain()
