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
This module contains the the definition of the control session class used by the control server.
"""


import logging
import json
from argparse import ArgumentError, HelpFormatter
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
    """
    This class defines the API for controlling a task pool instance from the outside.

    The commands received from a connected client are translated into method calls on the task pool instance.
    A subclass of the standard `argparse.ArgumentParser` is used to handle the input read from the stream.
    """

    def __init__(self, server: 'ControlServer', reader: StreamReader, writer: StreamWriter) -> None:
        """
        Instantiation should happen once a client connection to the control server has already been established.

        For more convenient/efficient access, some of the server's properties are saved in separate attributes.
        The argument parser is _not_ instantiated in the constructor. It requires a bit of client information during
        initialization, which is obtained in the `client_handshake` method; only there is the parser fully configured.

        Args:
            server:
                The instance of a `ControlServer` subclass starting the session.
            reader:
                The `asyncio.StreamReader` created when a client connected to the server.
            writer:
                The `asyncio.StreamWriter` created when a client connected to the server.
        """
        self._control_server: 'ControlServer' = server
        self._pool: Union[TaskPool, SimpleTaskPool] = server.pool
        self._client_class_name = server.client_class_name
        self._reader: StreamReader = reader
        self._writer: StreamWriter = writer
        self._parser: Optional[CommandParser] = None
        self._subparsers = None

    def _add_command(self, name: str, prog: str = None, short_help: str = None, long_help: str = None,
                     **kwargs) -> CommandParser:
        """
        Convenience method for adding a subparser (i.e. another command) to the main `CommandParser` instance.

        Will always pass the session's main `CommandParser` instance as the `parent` keyword-argument.

        Args:
            name:
                The command name; passed directly into the `add_parser` method.
            prog (optional):
                Also passed into the `add_parser` method as the corresponding keyword-argument. By default, is set
                equal to the `name` argument.
            short_help (optional):
                Passed into the `add_parser` method as the `help` keyword-argument, unless it is left empty and the
                `long_help` argument is present; in that case the `long_help` argument is passed as `help`.
            long_help (optional):
                Passed into the `add_parser` method as the `description` keyword-argument, unless it is left empty and
                the `short_help` argument is present; in that case the `short_help` argument is passed as `description`.
            **kwargs (optional):
                Any keyword-arguments to directly pass into the `add_parser` method.

        Returns:
            An instance of the `CommandParser` class representing the newly added control command.
        """
        if prog is None:
            prog = name
        kwargs.setdefault('help', short_help or long_help)
        kwargs.setdefault('description', long_help or short_help)
        return self._subparsers.add_parser(name, prog=prog, parent=self._parser, **kwargs)

    def _add_base_commands(self) -> None:
        """
        Adds the commands that are supported regardless of the specific subclass of `BaseTaskPool` controlled.

        These include commands mapping to the following pool methods:
            - __str__
            - pool_size (get/set property)
            - num_running
        """
        self._add_command(CMD.NAME, short_help=get_first_doc_line(self._pool.__class__.__str__))
        self._add_command(
            CMD.POOL_SIZE, 
            short_help="Get/set the maximum number of tasks in the pool.", 
            formatter_class=HelpFormatter
        ).add_optional_num_argument(
            default=None,
            help=f"If passed a number: {get_first_doc_line(self._pool.__class__.pool_size.fset)} "
                 f"If omitted: {get_first_doc_line(self._pool.__class__.pool_size.fget)}"
        )
        self._add_command(CMD.NUM_RUNNING, short_help=get_first_doc_line(self._pool.__class__.num_running.fget))

    def _add_simple_commands(self) -> None:
        """
        Adds the commands that are only supported, if a `SimpleTaskPool` object is controlled.

        These include commands mapping to the following pool methods:
            - start
            - stop
            - stop_all
            - func_name
        """
        self._add_command(
            CMD.START, short_help=get_first_doc_line(self._pool.__class__.start)
        ).add_optional_num_argument(
            help="Number of tasks to start."
        )
        self._add_command(
            CMD.STOP, short_help=get_first_doc_line(self._pool.__class__.stop)
        ).add_optional_num_argument(
            help="Number of tasks to stop."
        )
        self._add_command(CMD.STOP_ALL, short_help=get_first_doc_line(self._pool.__class__.stop_all))
        self._add_command(CMD.FUNC_NAME, short_help=get_first_doc_line(self._pool.__class__.func_name.fget))

    def _add_advanced_commands(self) -> None:
        """
        Adds the commands that are only supported, if a `TaskPool` object is controlled.

        These include commands mapping to the following pool methods:
            - ...
        """
        raise NotImplementedError

    def _init_parser(self, client_terminal_width: int) -> None:
        """
        Initializes and fully configures the `CommandParser` responsible for handling the input.

        Depending on what specific task pool class is controlled by the server, different commands are added.

        Args:
            client_terminal_width:
                The number of columns of the client's terminal to be able to nicely format messages from the parser.
        """
        parser_kwargs = {
            'prog': '',
            SESSION_PARSER_WRITER: self._writer,
            CLIENT_INFO.TERMINAL_WIDTH: client_terminal_width,
        }
        self._parser = CommandParser(**parser_kwargs)
        self._subparsers = self._parser.add_subparsers(title="Commands", dest=CMD.CMD)
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
        """
        This method must be invoked before starting any other client interaction.

        Client info is retrieved, server info is sent back, and the `CommandParser` is initialized and configured.
        """
        client_info = json.loads((await self._reader.read(SESSION_MSG_BYTES)).decode().strip())
        log.debug("%s connected", self._client_class_name)
        self._init_parser(client_info[CLIENT_INFO.TERMINAL_WIDTH])
        self._writer.write(str(self._pool).encode())
        await self._writer.drain()

    async def _write_function_output(self, func: Callable, *args, **kwargs) -> None:
        """
        Acts as a wrapper around a call to a specific task pool method.

        The method is called and any exception is caught and saved. If there is no output and no exception caught, a
        generic confirmation message is sent back to the client. Otherwise the output or a string representation of
        the exception caught is sent back.

        Args:
            func:
                Reference to the task pool method.
            *args (optional):
                Any positional arguments to call the method with.
            *+kwargs (optional):
                Any keyword-arguments to call the method with.
        """
        output = await return_or_exception(func, *args, **kwargs)
        self._writer.write(b"ok" if output is None else str(output).encode())

    async def _cmd_name(self, **_kwargs) -> None:
        """Maps to the `__str__` method of any task pool class."""
        log.debug("%s requests task pool name", self._client_class_name)
        await self._write_function_output(self._pool.__class__.__str__, self._pool)

    async def _cmd_pool_size(self, **kwargs) -> None:
        """Maps to the `pool_size` property of any task pool class."""
        num = kwargs.get(NUM)
        if num is None:
            log.debug("%s requests pool size", self._client_class_name)
            await self._write_function_output(self._pool.__class__.pool_size.fget, self._pool)
        else:
            log.debug("%s requests setting pool size to %s", self._client_class_name, num)
            await self._write_function_output(self._pool.__class__.pool_size.fset, self._pool, num)

    async def _cmd_num_running(self, **_kwargs) -> None:
        """Maps to the `num_running` property of any task pool class."""
        log.debug("%s requests number of running tasks", self._client_class_name)
        await self._write_function_output(self._pool.__class__.num_running.fget, self._pool)

    async def _cmd_start(self, **kwargs) -> None:
        """Maps to the `start` method of the `SimpleTaskPool` class."""
        num = kwargs[NUM]
        log.debug("%s requests starting %s %s", self._client_class_name, num, tasks_str(num))
        await self._write_function_output(self._pool.start, num)

    async def _cmd_stop(self, **kwargs) -> None:
        """Maps to the `stop` method of the `SimpleTaskPool` class."""
        num = kwargs[NUM]
        log.debug("%s requests stopping %s %s", self._client_class_name, num, tasks_str(num))
        await self._write_function_output(self._pool.stop, num)

    async def _cmd_stop_all(self, **_kwargs) -> None:
        """Maps to the `stop_all` method of the `SimpleTaskPool` class."""
        log.debug("%s requests stopping all tasks", self._client_class_name)
        await self._write_function_output(self._pool.stop_all)

    async def _cmd_func_name(self, **_kwargs) -> None:
        """Maps to the `func_name` method of the `SimpleTaskPool` class."""
        log.debug("%s requests pool function name", self._client_class_name)
        await self._write_function_output(self._pool.__class__.func_name.fget, self._pool)

    async def _execute_command(self, **kwargs) -> None:
        """
        Dynamically gets the correct `_cmd_...` method depending on the name of the command passed and executes it.

        Args:
            **kwargs:
                Must include the `CMD.CMD` key mapping the the command name. The rest of the keyword-arguments is
                simply passed into the method determined from the command name.
        """
        method = getattr(self, f'_cmd_{kwargs.pop(CMD.CMD).replace("-", "_")}')
        await method(**kwargs)

    async def _parse_command(self, msg: str) -> None:
        """
        Takes a message from the client and attempts to parse it.

        If a parsing error occurs, it is returned to the client. If the `HelpRequested` exception was raised by the
        `CommandParser`, nothing else happens. Otherwise, the `_execute_command` method is called with the entire
        dictionary of keyword-arguments returned by the `CommandParser` passed into it.

        Args:
            msg:
                The non-empty string read from the client stream.
        """
        try:
            kwargs = vars(self._parser.parse_args(msg.split(' ')))
        except ArgumentError as e:
            self._writer.write(str(e).encode())
            return
        except HelpRequested:
            return
        await self._execute_command(**kwargs)

    async def listen(self) -> None:
        """
        Enters the main control loop that only ends if either the server or the client disconnect.

        Messages from the client are read and passed into the `_parse_command` method, which handles the rest.
        This method should be called, when the client connection was established and the handshake was successful.
        It will obviously block indefinitely.
        """
        while self._control_server.is_serving():
            msg = (await self._reader.read(SESSION_MSG_BYTES)).decode().strip()
            if not msg:
                log.debug("%s disconnected", self._client_class_name)
                break
            await self._parse_command(msg)
            await self._writer.drain()
