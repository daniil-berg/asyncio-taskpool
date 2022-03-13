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
This module contains the the definition of the `ControlSession` class used by the control server.
"""


import logging
import json
from argparse import ArgumentError
from asyncio.streams import StreamReader, StreamWriter
from inspect import isfunction, signature
from typing import Callable, Optional, Union, TYPE_CHECKING

from ..constants import CLIENT_INFO, CMD, CMD_OK, SESSION_MSG_BYTES, STREAM_WRITER
from ..exceptions import HelpRequested
from ..helpers import return_or_exception
from ..pool import TaskPool, SimpleTaskPool
from .parser import ControlParser

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
        self._parser: Optional[ControlParser] = None

    async def _exec_method_and_respond(self, method: Callable, **kwargs) -> None:
        """
        Takes a pool method reference, executes it, and writes a response accordingly.

        If the first parameter is named `self`, the method will be called with the `_pool` instance as its first
        positional argument. If it returns nothing, the response upon successful execution will be `constants.CMD_OK`,
        otherwise the response written to the stream will be its return value (as an encoded string).

        Args:
            prop:
                The reference to the method defined on the `_pool` instance's class.
            **kwargs (optional):
                Must correspond to the arguments expected by the `method`.
                Correctly unpacks arbitrary-length positional and keyword-arguments.
        """
        log.warning("%s calls %s.%s", self._client_class_name, self._pool.__class__.__name__, method.__name__)
        normal_pos, var_pos = [], []
        for param in signature(method).parameters.values():
            if param.name == 'self':
                normal_pos.append(self._pool)
            elif param.kind in (param.POSITIONAL_OR_KEYWORD, param.POSITIONAL_ONLY):
                normal_pos.append(kwargs.pop(param.name))
            elif param.kind == param.VAR_POSITIONAL:
                var_pos = kwargs.pop(param.name)
        output = await return_or_exception(method, *normal_pos, *var_pos, **kwargs)
        self._writer.write(CMD_OK if output is None else str(output).encode())

    async def _exec_property_and_respond(self, prop: property, **kwargs) -> None:
        """
        Takes a pool property reference, executes its setter or getter, and writes a response accordingly.

        The property set/get method will always be called with the `_pool` instance as its first positional argument.

        Args:
            prop:
                The reference to the property defined on the `_pool` instance's class.
            **kwargs (optional):
                If not empty, the property setter is executed and the keyword arguments are passed along to it; the
                response upon successful execution will be `constants.CMD_OK`. Otherwise the property getter is
                executed and the response written to the stream will be its return value (as an encoded string).
        """
        if kwargs:
            log.warning("%s sets %s.%s", self._client_class_name, self._pool.__class__.__name__, prop.fset.__name__)
            await return_or_exception(prop.fset, self._pool, **kwargs)
            self._writer.write(CMD_OK)
        else:
            log.warning("%s gets %s.%s", self._client_class_name, self._pool.__class__.__name__, prop.fget.__name__)
            self._writer.write(str(await return_or_exception(prop.fget, self._pool)).encode())

    async def client_handshake(self) -> None:
        """
        This method must be invoked before starting any other client interaction.

        Client info is retrieved, server info is sent back, and the `ControlParser` is initialized and configured.
        """
        client_info = json.loads((await self._reader.read(SESSION_MSG_BYTES)).decode().strip())
        log.debug("%s connected", self._client_class_name)
        parser_kwargs = {
            STREAM_WRITER: self._writer,
            CLIENT_INFO.TERMINAL_WIDTH: client_info[CLIENT_INFO.TERMINAL_WIDTH],
            'prog': '',
            'usage': f'[-h] [{CMD}] ...'
        }
        self._parser = ControlParser(**parser_kwargs)
        self._parser.add_subparsers(title="Commands",
                                    metavar="(A command followed by '-h' or '--help' will show command-specific help.)")
        self._parser.add_class_commands(self._pool.__class__)
        self._writer.write(str(self._pool).encode())
        await self._writer.drain()

    async def _parse_command(self, msg: str) -> None:
        """
        Takes a message from the client and attempts to parse it.

        If a parsing error occurs, it is returned to the client. If the `HelpRequested` exception was raised by the
        `ControlParser`, nothing else happens. Otherwise, the appropriate `_exec...` method is called with the entire
        dictionary of keyword-arguments returned by the `ControlParser` passed into it.

        Args:
            msg: The non-empty string read from the client stream.
        """
        try:
            kwargs = vars(self._parser.parse_args(msg.split(' ')))
        except ArgumentError as e:
            self._writer.write(str(e).encode())
            return
        except HelpRequested:
            return
        command = kwargs.pop(CMD)
        if isfunction(command):
            await self._exec_method_and_respond(command, **kwargs)
        elif isinstance(command, property):
            await self._exec_property_and_respond(command, **kwargs)

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
