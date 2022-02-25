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
This module contains the task pool control server class definitions.
"""


import logging
from abc import ABC, abstractmethod
from asyncio import AbstractServer
from asyncio.exceptions import CancelledError
from asyncio.streams import StreamReader, StreamWriter
from asyncio.tasks import Task, create_task
from pathlib import Path
from typing import Optional, Union

from .client import ControlClient, UnixControlClient
from .pool import TaskPool, SimpleTaskPool
from .session import ControlSession
from .types import ConnectedCallbackT


log = logging.getLogger(__name__)


class ControlServer(ABC):  # TODO: Implement interface for normal TaskPool instances, not just SimpleTaskPool
    """
    Abstract base class for a task pool control server.

    This class acts as a wrapper around an async server instance and initializes a `ControlSession` upon a client
    connecting to it. The entire interface is defined within that session class.
    """
    _client_class = ControlClient

    @classmethod
    @property
    def client_class_name(cls) -> str:
        """Returns the name of the control client class matching the server class."""
        return cls._client_class.__name__

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

    def __init__(self, pool: Union[TaskPool, SimpleTaskPool], **server_kwargs) -> None:
        """
        Initializes by merely saving the internal attributes, but without starting the server yet.
        The task pool must be passed here and can not be set/changed afterwards. This means a control server is always
        tied to one specific task pool.

        Args:
            pool:
                An instance of a `BaseTaskPool` subclass to tie the server to.
            **server_kwargs (optional):
                Keyword arguments that will be passed into the function that starts the server.
        """
        self._pool: Union[TaskPool, SimpleTaskPool] = pool
        self._server_kwargs = server_kwargs
        self._server: Optional[AbstractServer] = None

    @property
    def pool(self) -> Union[TaskPool, SimpleTaskPool]:
        """Read-only property for accessing the task pool instance controlled by the server."""
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
        This method actually starts the server and begins listening to client connections on the specified interface.
        It should never block because the serving will be performed in a separate task.
        """
        log.debug("Starting %s...", self.__class__.__name__)
        self._server = await self._get_server_instance(self._client_connected_cb, **self._server_kwargs)
        return create_task(self._serve_forever())


class UnixControlServer(ControlServer):
    """Task pool control server class that exposes a unix socket for control clients to connect to."""
    _client_class = UnixControlClient

    def __init__(self, pool: SimpleTaskPool, **server_kwargs) -> None:
        from asyncio.streams import start_unix_server
        self._start_unix_server = start_unix_server
        self._socket_path = Path(server_kwargs.pop('path'))
        super().__init__(pool, **server_kwargs)

    async def _get_server_instance(self, client_connected_cb: ConnectedCallbackT, **kwargs) -> AbstractServer:
        server = await self._start_unix_server(client_connected_cb, self._socket_path, **kwargs)
        log.debug("Opened socket '%s'", str(self._socket_path))
        return server

    def _final_callback(self) -> None:
        """Removes the unix socket on which the server was listening."""
        self._socket_path.unlink()
        log.debug("Removed socket '%s'", str(self._socket_path))
