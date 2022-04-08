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
Classes of control clients for a simply interface to a task pool control server.
"""


import json
import shutil
import sys
from abc import ABC, abstractmethod
from asyncio.streams import StreamReader, StreamWriter, open_connection
from pathlib import Path
from typing import Optional, Union

from ..internals.constants import CLIENT_INFO, SESSION_MSG_BYTES
from ..internals.types import ClientConnT, PathT


__all__ = [
    'ControlClient',
    'TCPControlClient',
    'UnixControlClient',
    'CLIENT_EXIT'
]


CLIENT_EXIT = 'exit'


class ControlClient(ABC):
    """
    Abstract base class for a simple implementation of a pool control client.

    Since the server's control interface is simply expecting commands to be sent, any process able to connect to the
    TCP or UNIX socket and issue the relevant commands (and optionally read the responses) will work just as well.
    This is a minimal working implementation.
    """

    @staticmethod
    def _client_info() -> dict:
        """Returns a dictionary of client information relevant for the handshake with the server."""
        return {CLIENT_INFO.TERMINAL_WIDTH: shutil.get_terminal_size().columns}

    @abstractmethod
    async def _open_connection(self, **kwargs) -> ClientConnT:
        """
        Tries to connect to a socket using the provided arguments and return the associated reader-writer-pair.

        This method will be invoked by the public `start()` method with the pre-defined internal `_conn_kwargs`
        (unpacked) as keyword-arguments.
        This method should return either a tuple of `asyncio.StreamReader` and `asyncio.StreamWriter` or a tuple of
        `None` and `None`, if it failed to establish the defined connection.
        """
        raise NotImplementedError

    def __init__(self, **conn_kwargs) -> None:
        """Simply stores the keyword-arguments for opening the connection."""
        self._conn_kwargs = conn_kwargs
        self._connected: bool = False

    async def _server_handshake(self, reader: StreamReader, writer: StreamWriter) -> None:
        """
        Performs the first interaction with the server providing it with the necessary client information.

        Upon completion, the server's info is printed.

        Args:
            reader: The `asyncio.StreamReader` returned by the `_open_connection()` method
            writer: The `asyncio.StreamWriter` returned by the `_open_connection()` method
        """
        self._connected = True
        writer.write(json.dumps(self._client_info()).encode())
        await writer.drain()
        print("Connected to", (await reader.read(SESSION_MSG_BYTES)).decode())
        print("Type '-h' to get help and usage instructions for all available commands.\n")

    def _get_command(self, writer: StreamWriter) -> Optional[str]:
        """
        Prompts the user for input and either returns it (after cleaning it up) or `None` in special cases.

        Args:
            writer: The `asyncio.StreamWriter` returned by the `_open_connection()` method

        Returns:
            `None`, if either `Ctrl+C` was hit, an empty or whitespace-only string was entered, or the user wants the
            client to disconnect; otherwise, returns the user's input, stripped of leading and trailing spaces and
            converted to lowercase.
        """
        try:
            cmd = input("> ").strip().lower()
        except EOFError:  # Ctrl+D shall be equivalent to the :const:`CLIENT_EXIT` command.
            cmd = CLIENT_EXIT
        except KeyboardInterrupt:  # Ctrl+C shall simply reset to the input prompt.
            print()
            return
        if cmd == CLIENT_EXIT:
            writer.close()
            self._connected = False
            return
        return cmd or None  # will be None if `cmd` is an empty string

    async def _interact(self, reader: StreamReader, writer: StreamWriter) -> None:
        """
        Reacts to the user's command, potentially performing a back-and-forth interaction with the server.

        If `_get_command` returns `None`, this may imply that the client disconnected, but may also just be `Ctrl+C`.
        If an actual command is retrieved, it is written to the stream, a response is awaited and eventually printed.

        Args:
            reader: The `asyncio.StreamReader` returned by the `_open_connection()` method
            writer: The `asyncio.StreamWriter` returned by the `_open_connection()` method
        """
        cmd = self._get_command(writer)
        if cmd is None:
            return
        try:
            # Send the command to the server.
            writer.write(cmd.encode())
            await writer.drain()
        except ConnectionError as e:
            self._connected = False
            print(e, file=sys.stderr)
            return
        # Await the server's response, then print it.
        print((await reader.read(SESSION_MSG_BYTES)).decode())

    async def start(self) -> None:
        """
        Opens connection, performs handshake, and enters interaction loop.

        An input prompt is presented to the user and any input is sent (encoded) to the connected server.
        One exception is the :const:`CLIENT_EXIT` command (equivalent to Ctrl+D), which merely closes the connection.

        If the connection can not be established, an error message is printed to `stderr` and the method returns.
        If either the exit command is issued or the connection to the server is lost during the interaction loop,
        the method returns and prints out a disconnected-message.
        """
        reader, writer = await self._open_connection(**self._conn_kwargs)
        if reader is None:
            print("Failed to connect.", file=sys.stderr)
            return
        await self._server_handshake(reader, writer)
        while self._connected:
            await self._interact(reader, writer)
        print("Disconnected from control server.")


class TCPControlClient(ControlClient):
    """Task pool control client for connecting to a :class:`TCPControlServer`."""

    def __init__(self, host: str, port: Union[int, str], **conn_kwargs) -> None:
        """`host` and `port` are expected as non-optional connection arguments."""
        self._host = host
        self._port = port
        super().__init__(**conn_kwargs)

    async def _open_connection(self, **kwargs) -> ClientConnT:
        """
        Wrapper around the `asyncio.open_connection` function.

        Returns a tuple of `None` and `None`, if the connection can not be established;
        otherwise, the stream-reader and -writer tuple is returned.
        """
        try:
            return await open_connection(self._host, self._port, **kwargs)
        except ConnectionError as e:
            print(str(e), file=sys.stderr)
            return None, None


class UnixControlClient(ControlClient):
    """Task pool control client for connecting to a :class:`UnixControlServer`."""

    def __init__(self, socket_path: PathT, **conn_kwargs) -> None:
        """`socket_path` is expected as a non-optional connection argument."""
        from asyncio.streams import open_unix_connection
        self._open_unix_connection = open_unix_connection
        self._socket_path = Path(socket_path)
        super().__init__(**conn_kwargs)

    async def _open_connection(self, **kwargs) -> ClientConnT:
        """
        Wrapper around the `asyncio.open_unix_connection` function.

        Returns a tuple of `None` and `None`, if the socket is not found at the pre-defined path;
        otherwise, the stream-reader and -writer tuple is returned.
        """
        try:
            return await self._open_unix_connection(self._socket_path, **kwargs)
        except FileNotFoundError:
            print("No socket at", self._socket_path, file=sys.stderr)
            return None, None
