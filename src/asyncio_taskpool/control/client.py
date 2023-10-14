"""Control client classes for interfacing with a task pool control server."""

from __future__ import annotations

import json
import shutil
import sys
from abc import ABC, abstractmethod
from asyncio.streams import StreamReader, StreamWriter, open_connection
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..internals.constants import CLIENT_INFO, SESSION_MSG_BYTES

if TYPE_CHECKING:
    from ..internals.types import ClientConnT, PathT

__all__ = [
    "ControlClient",
    "TCPControlClient",
    "UnixControlClient",
    "CLIENT_EXIT",
]


CLIENT_EXIT = "exit"


class ControlClient(ABC):
    """
    Abstract base class for a simple implementation of a pool control client.

    Since the server control interface is simply expecting commands to be sent,
    any process able to connect to the TCP or UNIX socket and issue the relevant
    commands (and optionally read the responses) will work just as well.
    This is a minimal working implementation.
    """

    @staticmethod
    def _client_info() -> dict[str, Any]:
        """Returns the handshake-relevant client information as a dictionary."""
        return {CLIENT_INFO.TERMINAL_WIDTH: shutil.get_terminal_size().columns}

    @abstractmethod
    async def _open_connection(self, **kwargs: Any) -> ClientConnT:
        """
        Tries to connect to a socket using the provided arguments.

        This method will be invoked by the public `start()` method with the
        pre-defined internal `_conn_kwargs` (unpacked) as keyword-arguments.
        This method should return either a tuple of `asyncio.StreamReader` and
        `asyncio.StreamWriter` or a tuple of `None` and `None`, if it failed to
        establish the defined connection.

        Returns:
            The associated `StreamReader`-`StreamWriter`-pair, if successful;
            `None, None`, if unsuccessful.
        """
        raise NotImplementedError

    def __init__(self, **conn_kwargs: Any) -> None:
        """Simply stores the keyword-arguments for opening the connection."""
        self._conn_kwargs = conn_kwargs
        self._connected: bool = False

    async def _server_handshake(
        self,
        reader: StreamReader,
        writer: StreamWriter,
    ) -> None:
        """
        Provides the server with the necessary client information.

        Upon completion, the server's info is printed.

        Args:
            reader:
                The `asyncio.StreamReader` returned by `_open_connection`
            writer:
                The `asyncio.StreamWriter` returned by `_open_connection`
        """
        self._connected = True
        writer.write(json.dumps(self._client_info()).encode())
        writer.write(b"\n")
        await writer.drain()
        print("Connected to", (await reader.read(SESSION_MSG_BYTES)).decode())
        print(
            "Type '-h' to get help and usage instructions for all available commands.\n"
        )

    def _get_command(self, writer: StreamWriter) -> str | None:
        """
        Prompts the user for input and returns it (after cleaning it up).

        Args:
            writer:
                The `asyncio.StreamWriter` returned by `_open_connection`

        Returns:
            `None`, if either `Ctrl+C` was hit, an empty or whitespace-only
            string was entered, or the user wants the client to disconnect;
            otherwise, returns the user's input, stripped of leading and
            trailing spaces and converted to lowercase.
        """
        try:
            cmd = input("> ").strip().lower()
        except (
            EOFError
        ):  # Ctrl+D shall be equivalent to the :const:`CLIENT_EXIT` command.
            cmd = CLIENT_EXIT
        except (
            KeyboardInterrupt
        ):  # Ctrl+C shall simply reset to the input prompt.
            print()
            return None
        if cmd == CLIENT_EXIT:
            writer.close()
            self._connected = False
            return None
        return cmd or None  # will be None if `cmd` is an empty string

    async def _interact(
        self,
        reader: StreamReader,
        writer: StreamWriter,
    ) -> None:
        """
        Reacts to the user's command, performing an interaction with the server.

        If `_get_command` returns `None`, this may imply that the client
        disconnected, but may also just be `Ctrl+C`.
        If an actual command is retrieved, it is written to the stream,
        a response is awaited and eventually printed.

        Args:
            reader:
                The `asyncio.StreamReader` returned by `_open_connection`
            writer:
                The `asyncio.StreamWriter` returned by `_open_connection`
        """
        cmd = self._get_command(writer)
        if cmd is None:
            return
        try:
            # Send the command to the server.
            writer.write(cmd.encode())
            writer.write(b"\n")
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

        An input prompt is presented to the user and any input is sent (encoded)
        to the connected server. One exception is the :const:`CLIENT_EXIT`
        command (equivalent to Ctrl+D), which merely closes the connection.

        If the connection can not be established, an error message is printed to
        `stderr` and the method returns. If either the exit command is issued or
        the connection to the server is lost during the interaction loop,
        the method returns and prints out a disconnected-message.
        """
        reader, writer = await self._open_connection(**self._conn_kwargs)
        if reader is None or writer is None:
            print("Failed to connect.", file=sys.stderr)
            return
        await self._server_handshake(reader, writer)
        while self._connected:
            await self._interact(reader, writer)
        print("Disconnected from control server.")


class TCPControlClient(ControlClient):
    """Pool control client for connecting to a :class:`TCPControlServer`."""

    def __init__(self, host: str, port: int | str, **conn_kwargs: Any) -> None:
        """`host` and `port` are non-optional connection arguments."""
        self._host = host
        self._port = port
        super().__init__(**conn_kwargs)

    async def _open_connection(self, **kwargs: Any) -> ClientConnT:
        """
        Wrapper around the `asyncio.open_connection` function.

        Returns a tuple of `None` and `None`, if the connection can not be
        established; otherwise, the reader-writer-tuple is returned.
        """
        try:
            return await open_connection(self._host, self._port, **kwargs)
        except ConnectionError as e:
            print(str(e), file=sys.stderr)
            return None, None


class UnixControlClient(ControlClient):
    """Pool control client for connecting to a :class:`UnixControlServer`."""

    def __init__(self, socket_path: PathT, **conn_kwargs: Any) -> None:
        """`socket_path` is expected as a non-optional connection argument."""
        from asyncio.streams import open_unix_connection

        self._open_unix_connection = open_unix_connection
        self._socket_path = Path(socket_path)
        super().__init__(**conn_kwargs)

    async def _open_connection(self, **kwargs: Any) -> ClientConnT:
        """
        Wrapper around the `asyncio.open_unix_connection` function.

        Returns a tuple of `None` and `None`, if the socket is not found at the
        pre-defined path; otherwise, the reader-writer-tuple is returned.
        """
        try:
            return await self._open_unix_connection(self._socket_path, **kwargs)
        except FileNotFoundError:
            print("No socket at", self._socket_path, file=sys.stderr)
            return None, None


# Using `print` in the reference client is fine.
# ruff: noqa: T201
