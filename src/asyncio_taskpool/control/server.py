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
Task pool control server class definitions.
"""


import logging
from abc import ABC, abstractmethod
from asyncio import AbstractServer
from asyncio.exceptions import CancelledError
from asyncio.streams import StreamReader, StreamWriter, start_server
from asyncio.tasks import Task, create_task
from pathlib import Path
from typing import Optional, Union

from .client import ControlClient, TCPControlClient, UnixControlClient
from .session import ControlSession
from ..pool import AnyTaskPoolT
from ..internals.helpers import classmethod
from ..internals.types import ConnectedCallbackT, PathT


__all__ = ['ControlServer', 'TCPControlServer', 'UnixControlServer']


log = logging.getLogger(__name__)


class ControlServer(ABC):
    """
    Abstract base class for a task pool control server.

    This class acts as a wrapper around an async server instance and initializes a
    :class:`ControlSession <asyncio_taskpool.control.session.ControlSession>` once a client connects to it.
    The interface is defined within the session class.
    """
    _client_class = ControlClient

    @classmethod
    @property
    def client_class_name(cls) -> str:
        """Returns the name of the matching control client class."""
        return cls._client_class.__name__

    def __init__(self, pool: AnyTaskPoolT, **server_kwargs) -> None:
        """
        Merely sets internal attributes, but does not start the server yet.
        The task pool must be passed here and can not be set/changed afterwards. This means a control server is always
        tied to one specific task pool.

        Args:
            pool:
                An instance of a `BaseTaskPool` subclass to tie the server to.
            **server_kwargs (optional):
                Keyword arguments that will be passed into the function that starts the server.
        """
        self._pool: AnyTaskPoolT = pool
        self._server_kwargs = server_kwargs
        self._server: Optional[AbstractServer] = None

    @property
    def pool(self) -> AnyTaskPoolT:
        """The task pool instance controlled by the server."""
        return self._pool

    def is_serving(self) -> bool:
        """Wrapper around the `asyncio.Server.is_serving` method."""
        return self._server.is_serving()

    async def _client_connected_cb(self, reader: StreamReader, writer: StreamWriter) -> None:
        """
        The universal client callback that will be passed into the `_get_server_instance` method.
        Instantiates a control session, performs the client handshake, and enters the session's `listen` loop.
        """
        session = ControlSession(self, reader, writer)
        await session.client_handshake()
        await session.listen()

    @abstractmethod
    async def _get_server_instance(self, client_connected_cb: ConnectedCallbackT, **kwargs) -> AbstractServer:
        """
        Initializes, starts, and returns an async server instance (Unix or TCP type).

        Args:
            client_connected_cb:
                The callback for when a client connects to the server (as per `asyncio.start_server` or
                `asyncio.start_unix_server`); will always be the internal `_client_connected_cb` method.
            **kwargs (optional):
                Keyword arguments to pass into the function that starts the server.

        Returns:
            The running server object (a type of `asyncio.Server`).
        """
        raise NotImplementedError

    @abstractmethod
    def _final_callback(self) -> None:
        """The method to run after the server's `serve_forever` methods ends for whatever reason."""
        raise NotImplementedError

    async def _serve_forever(self) -> None:
        """
        To be run as an `asyncio.Task` by the following method.
        Serves as a wrapper around the the `asyncio.Server.serve_forever` method that ensures that the `_final_callback`
        method is called, when the former method ends for whatever reason.
        """
        try:
            async with self._server:
                await self._server.serve_forever()
        except CancelledError:
            log.debug("%s stopped", self.__class__.__name__)
        finally:
            self._final_callback()

    async def serve_forever(self) -> Task:
        """
        Starts the server and begins listening to client connections.

        It should never block because the serving will be performed in a separate task.

        Returns:
            The forever serving task. To stop the server, this task should be cancelled.
        """
        log.debug("Starting %s...", self.__class__.__name__)
        self._server = await self._get_server_instance(self._client_connected_cb, **self._server_kwargs)
        return create_task(self._serve_forever())


class TCPControlServer(ControlServer):
    """Exposes a TCP socket for control clients to connect to."""
    _client_class = TCPControlClient

    def __init__(self, pool: AnyTaskPoolT, host: str, port: Union[int, str], **server_kwargs) -> None:
        """`host` and `port` are expected as non-optional server arguments."""
        self._host = host
        self._port = port
        super().__init__(pool, **server_kwargs)

    async def _get_server_instance(self, client_connected_cb: ConnectedCallbackT, **kwargs) -> AbstractServer:
        server = await start_server(client_connected_cb, self._host, self._port, **kwargs)
        log.debug("Opened socket at %s:%s", self._host, self._port)
        return server

    def _final_callback(self) -> None:
        log.debug("Closed socket at %s:%s", self._host, self._port)


class UnixControlServer(ControlServer):
    """Exposes a unix socket for control clients to connect to."""
    _client_class = UnixControlClient

    def __init__(self, pool: AnyTaskPoolT, socket_path: PathT, **server_kwargs) -> None:
        """`socket_path` is expected as a non-optional server argument."""
        from asyncio.streams import start_unix_server
        self._start_unix_server = start_unix_server
        self._socket_path = Path(socket_path)
        super().__init__(pool, **server_kwargs)

    async def _get_server_instance(self, client_connected_cb: ConnectedCallbackT, **kwargs) -> AbstractServer:
        server = await self._start_unix_server(client_connected_cb, self._socket_path, **kwargs)
        log.debug("Opened socket '%s'", str(self._socket_path))
        return server

    def _final_callback(self) -> None:
        """Removes the unix socket on which the server was listening."""
        self._socket_path.unlink()
        log.debug("Removed socket '%s'", str(self._socket_path))
