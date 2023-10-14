"""
Definition of the :class:`ControlSession` used by a :class:`ControlServer`.

It should not be considered part of the public API.
"""

from __future__ import annotations

import json
import logging
from argparse import ArgumentError
from inspect import isfunction, signature
from io import StringIO
from typing import TYPE_CHECKING, Any, Callable

from ..exceptions import (
    CommandError,
    HelpRequested,
    ParserError,
    ParserNotInitialized,
)
from ..internals.constants import CLIENT_INFO, CMD, CMD_OK
from ..internals.helpers import return_or_exception
from .parser import ControlParser

if TYPE_CHECKING:
    from asyncio.streams import StreamReader, StreamWriter

    from ..pool import AnyTaskPoolT
    from .server import ClientT, ControlServer


__all__ = ["ControlSession"]


log = logging.getLogger(__name__)


class ControlSession:
    """
    Manages a single control session between a server and a client.

    The commands received from a connected client are translated into method
    calls on the task pool instance. A subclass of the standard
    :class:`argparse.ArgumentParser` is used to handle the input read from
    the stream.
    """

    def __init__(
        self,
        server: "ControlServer[ClientT]",
        reader: StreamReader,
        writer: StreamWriter,
    ) -> None:
        """
        Connection to the control server should already been established.

        For more convenient/efficient access, some of the server's properties
        are saved in separate attributes. The argument parser is _not_
        instantiated in the constructor. It requires a bit of client information
        during initialization, which is obtained in the `client_handshake`
        method; only there is the parser fully configured.

        Args:
            server:
                :class:`ControlServer` subclass instance starting the session.
            reader:
                `asyncio.StreamReader` created when a client connected.
            writer:
                `asyncio.StreamWriter` created when a client connected.
        """
        self._control_server: "ControlServer[ClientT]" = server
        self._pool: AnyTaskPoolT = server.pool
        self._client_class_name = server.client_class_name
        self._reader: StreamReader = reader
        self._writer: StreamWriter = writer
        self._parser: ControlParser | None = None
        self._response_buffer: StringIO = StringIO()

    async def _exec_method_and_respond(
        self,
        method: Callable[..., Any],
        **kwargs: Any,
    ) -> None:
        """
        Takes a method, executes it, and writes a response accordingly.

        If the first parameter is named `self`, the method will be called with
        the `_pool` instance as its first positional argument.
        If it returns nothing, the response upon successful execution will be
        :const:`constants.CMD_OK`, otherwise the response written to the stream
        will be its return value (as an encoded string).

        Args:
            method:
                Reference to the method defined on the `_pool` instance's class.
            **kwargs (optional):
                Must correspond to the arguments expected by the `method`.
                Unpacks arbitrary-length positional and keyword-arguments.
        """
        log.debug(
            "%s calls %s.%s",
            self._client_class_name,
            self._pool.__class__.__name__,
            method.__name__,
        )
        normal_pos, var_pos = [], []
        for param in signature(method).parameters.values():
            if param.name == "self":
                normal_pos.append(self._pool)
            elif param.kind in (
                param.POSITIONAL_OR_KEYWORD,
                param.POSITIONAL_ONLY,
            ):
                normal_pos.append(kwargs.pop(param.name))
            elif param.kind == param.VAR_POSITIONAL:
                var_pos = kwargs.pop(param.name)
        output = await return_or_exception(
            method, *normal_pos, *var_pos, **kwargs
        )
        self._response_buffer.write(
            CMD_OK.decode() if output is None else str(output)
        )

    async def _exec_property_and_respond(
        self,
        prop: property,
        **kwargs: Any,
    ) -> None:
        """
        Executes a property setter/getter, and writes a response accordingly.

        The property set/get method will always be called with the `_pool`
        instance as its first positional argument.

        Args:
            prop:
                Property defined on the `_pool` instance's class.
            **kwargs (optional):
                If not empty, the property setter is executed and the keyword
                arguments are passed along to it; the response upon successful
                execution will be :const:`constants.CMD_OK`. Otherwise the
                property getter is executed and the response written to the
                stream will be its return value (as an encoded string).
        """
        if kwargs:
            if prop.fset is None:
                raise TypeError("Property must have a setter")  # noqa: TRY003
            log.debug(
                "%s sets %s.%s",
                self._client_class_name,
                self._pool.__class__.__name__,
                prop.fset.__name__,
            )
            await return_or_exception(prop.fset, self._pool, **kwargs)  # type: ignore[call-arg]
            self._response_buffer.write(CMD_OK.decode())
        else:
            if prop.fget is None:
                raise TypeError("Property must have a getter")  # noqa: TRY003
            log.debug(
                "%s gets %s.%s",
                self._client_class_name,
                self._pool.__class__.__name__,
                prop.fget.__name__,
            )
            self._response_buffer.write(
                str(await return_or_exception(prop.fget, self._pool))
            )

    async def client_handshake(self) -> None:
        """
        Must be invoked before starting any other client interaction.

        Client info is retrieved, server info is sent back, and the
        :class:`ControlParser <asyncio_taskpool.control.parser.ControlParser>`
        is set up.
        """
        msg = (await self._reader.readline()).decode().strip()
        client_info = json.loads(msg)
        log.debug("%s connected", self._client_class_name)
        parser_kwargs = {
            "stream": self._response_buffer,
            CLIENT_INFO.TERMINAL_WIDTH: client_info[CLIENT_INFO.TERMINAL_WIDTH],
            "prog": "",
            "usage": f"[-h] [{CMD}] ...",
        }
        self._parser = ControlParser(**parser_kwargs)
        self._parser.add_subparsers(
            title="Commands",
            metavar="(A command followed by '-h' or '--help' will show command-specific help.)",
        )
        self._parser.add_class_commands(self._pool.__class__)
        self._writer.write(str(self._pool).encode() + b"\n")
        await self._writer.drain()

    async def _parse_command(self, msg: str) -> None:
        """
        Takes a message from the client and attempts to parse it.

        If a parsing error occurs, it is returned to the client. If the
        :exc:`HelpRequested` exception was raised by the :class:`ControlParser`,
        nothing else happens. Otherwise, the appropriate `_exec...` method is
        called with the entire dictionary of keyword-arguments returned by the
        :class:`ControlParser` passed into it.

        Args:
            msg: The non-empty string read from the client stream.
        """
        if self._parser is None:
            raise ParserNotInitialized
        try:
            kwargs = vars(self._parser.parse_args(msg.split(" ")))
        except ArgumentError as e:
            log.debug("%s got an ArgumentError", self._client_class_name)
            self._response_buffer.write(str(e))
            return
        except (HelpRequested, ParserError):
            log.debug("%s received usage help", self._client_class_name)
            return
        command = kwargs.pop(CMD)
        if isfunction(command):
            await self._exec_method_and_respond(command, **kwargs)
        elif isinstance(command, property):
            await self._exec_property_and_respond(command, **kwargs)
        else:
            self._response_buffer.write(
                str(CommandError(f"Unknown command object: {command}"))
            )

    async def listen(self) -> None:
        """
        Enters the main control loop listening to client input.

        This method only returns if either the server or the client disconnect.
        Messages from the client are read, parsed, and turned into pool commands
        (if possible). This method should be called, when the client connection
        was established and the handshake was successful.
        It will obviously block indefinitely.
        """
        while self._control_server.is_serving():
            msg = (await self._reader.readline()).decode().strip()
            if not msg:
                log.debug("%s disconnected", self._client_class_name)
                break
            await self._parse_command(msg)
            response = self._response_buffer.getvalue() + "\n"
            self._response_buffer.seek(0)
            self._response_buffer.truncate()
            self._writer.write(response.encode())
            await self._writer.drain()
