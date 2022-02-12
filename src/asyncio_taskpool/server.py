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
from asyncio.streams import StreamReader, StreamWriter, start_unix_server
from asyncio.tasks import Task, create_task
from pathlib import Path
from typing import Optional, Union

from .client import ControlClient, UnixControlClient
from .pool import TaskPool, SimpleTaskPool
from .session import ControlSession


log = logging.getLogger(__name__)


class ControlServer(ABC):  # TODO: Implement interface for normal TaskPool instances, not just SimpleTaskPool
    _client_class = ControlClient

    @classmethod
    @property
    def client_class_name(cls) -> str:
        return cls._client_class.__name__

    @abstractmethod
    async def get_server_instance(self, client_connected_cb, **kwargs) -> AbstractServer:
        raise NotImplementedError

    @abstractmethod
    def final_callback(self) -> None:
        raise NotImplementedError

    def __init__(self, pool: Union[TaskPool, SimpleTaskPool], **server_kwargs) -> None:
        self._pool: Union[TaskPool, SimpleTaskPool] = pool
        self._server_kwargs = server_kwargs
        self._server: Optional[AbstractServer] = None

    def __str__(self) -> str:
        return f"{self.__class__.__name__} for {self._pool}"

    @property
    def pool(self) -> Union[TaskPool, SimpleTaskPool]:
        return self._pool

    def is_serving(self) -> bool:
        return self._server.is_serving()

    async def _client_connected_cb(self, reader: StreamReader, writer: StreamWriter) -> None:
        session = ControlSession(self, reader, writer)
        await session.client_handshake()
        await session.listen()

    async def _serve_forever(self) -> None:
        try:
            async with self._server:
                await self._server.serve_forever()
        except CancelledError:
            log.debug("%s stopped", self.__class__.__name__)
        finally:
            self.final_callback()

    async def serve_forever(self) -> Task:
        log.debug("Starting %s...", self.__class__.__name__)
        self._server = await self.get_server_instance(self._client_connected_cb, **self._server_kwargs)
        return create_task(self._serve_forever())


class UnixControlServer(ControlServer):
    _client_class = UnixControlClient

    def __init__(self, pool: SimpleTaskPool, **server_kwargs) -> None:
        self._socket_path = Path(server_kwargs.pop('path'))
        super().__init__(pool, **server_kwargs)

    async def get_server_instance(self, client_connected_cb, **kwargs) -> AbstractServer:
        srv = await start_unix_server(client_connected_cb, self._socket_path, **kwargs)
        log.debug("Opened socket '%s'", str(self._socket_path))
        return srv

    def final_callback(self) -> None:
        self._socket_path.unlink()
        log.debug("Removed socket '%s'", str(self._socket_path))
