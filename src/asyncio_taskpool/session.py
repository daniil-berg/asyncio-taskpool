import logging
import json
from argparse import ArgumentError, HelpFormatter, Namespace
from asyncio.streams import StreamReader, StreamWriter
from typing import Callable, Optional, Union, TYPE_CHECKING

from .constants import CMD, SESSION_PARSER_WRITER, SESSION_MSG_BYTES, CLIENT_INFO
from .exceptions import HelpRequested, NotATaskPool, UnknownTaskPoolClass
from .helpers import get_first_doc_line, return_or_exception, tasks_str
from .pool import BaseTaskPool, TaskPool, SimpleTaskPool
from .session_parser import CommandParser, NUM

if TYPE_CHECKING:
    from .server import ControlServer


log = logging.getLogger(__name__)


class ControlSession:
    def __init__(self, server: 'ControlServer', reader: StreamReader, writer: StreamWriter) -> None:
        self._control_server: 'ControlServer' = server
        self._pool: Union[TaskPool, SimpleTaskPool] = server.pool
        self._client_class_name = server.client_class_name
        self._reader: StreamReader = reader
        self._writer: StreamWriter = writer
        self._parser: Optional[CommandParser] = None
        self._subparsers = None

    def _add_parser_command(self, name: str, prog: str = None, short_help: str = None, long_help: str = None, 
                            **kwargs) -> CommandParser:
        if prog is None:
            prog = name
        kwargs.setdefault('help', short_help or long_help)
        kwargs.setdefault('description', long_help or short_help)
        return self._subparsers.add_parser(name, prog=prog, parent=self._parser, **kwargs)

    def _add_base_commands(self) -> None:
        self._subparsers = self._parser.add_subparsers(title="Commands", dest=CMD.CMD)
        self._add_parser_command(CMD.NAME, short_help=get_first_doc_line(self._pool.__class__.__str__))
        self._add_parser_command(
            CMD.POOL_SIZE, 
            short_help="Get/set the maximum number of tasks in the pool.", 
            formatter_class=HelpFormatter
        ).add_optional_num_argument(
            default=None,
            help=f"If passed a number: {get_first_doc_line(self._pool.__class__.pool_size.fset)} "
                 f"If omitted: {get_first_doc_line(self._pool.__class__.pool_size.fget)}"
        )
        self._add_parser_command(
            CMD.NUM_RUNNING, short_help=get_first_doc_line(self._pool.__class__.num_running.fget)
        )

    def _add_simple_commands(self) -> None:
        self._add_parser_command(
            CMD.START, short_help=get_first_doc_line(self._pool.__class__.start)
        ).add_optional_num_argument(
            help="Number of tasks to start."
        )
        self._add_parser_command(
            CMD.STOP, short_help=get_first_doc_line(self._pool.__class__.stop)
        ).add_optional_num_argument(
            help="Number of tasks to stop."
        )
        self._add_parser_command(CMD.STOP_ALL, short_help=get_first_doc_line(self._pool.__class__.stop_all))
        self._add_parser_command(
            CMD.FUNC_NAME, short_help=get_first_doc_line(self._pool.__class__.func_name.fget)
        )

    def _add_advanced_commands(self) -> None:
        raise NotImplementedError

    def _init_parser(self, client_terminal_width: int) -> None:
        parser_kwargs = {
            'prog': '',
            SESSION_PARSER_WRITER: self._writer,
            CLIENT_INFO.TERMINAL_WIDTH: client_terminal_width,
        }
        self._parser = CommandParser(**parser_kwargs)
        self._add_base_commands()
        if isinstance(self._pool, TaskPool):
            self._add_advanced_commands()
        elif isinstance(self._pool, SimpleTaskPool):
            self._add_simple_commands()
        elif isinstance(self._pool, BaseTaskPool):
            raise UnknownTaskPoolClass(f"No interface defined for {self._pool.__class__.__name__}")
        else:
            raise NotATaskPool(f"Not a task pool instance: {self._pool}")

    async def client_handshake(self) -> None:
        client_info = json.loads((await self._reader.read(SESSION_MSG_BYTES)).decode().strip())
        log.debug("%s connected", self._client_class_name)
        self._init_parser(client_info[CLIENT_INFO.TERMINAL_WIDTH])
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
            await self._write_function_output(self._pool.__class__.pool_size.fset, self._pool, num)

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
        cmd: str = args.pop(CMD.CMD, None)
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
            msg = (await self._reader.read(SESSION_MSG_BYTES)).decode().strip()
            if not msg:
                log.debug("%s disconnected", self._client_class_name)
                break
            await self._parse_command(msg)
            await self._writer.drain()
