"""Task pool control server class definitions."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from asyncio.exceptions import CancelledError
from asyncio.streams import StreamReader, StreamWriter, start_server
from asyncio.tasks import Task, create_task
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from ..exceptions import ServerNotInitialized
from ..internals.helpers import classmethod
from .client import ControlClient, TCPControlClient, UnixControlClient
from .session import ControlSession

if TYPE_CHECKING:
    from asyncio import AbstractServer

    from ..internals.types import ConnectedCallbackT, PathT
    from ..pool import AnyTaskPoolT

__all__ = ["ControlServer", "TCPControlServer", "UnixControlServer"]

ClientT = TypeVar("ClientT", bound=ControlClient)

log = logging.getLogger(__name__)


class ControlServer(ABC, Generic[ClientT]):
    """
    Abstract base class for a task pool control server.

    This class acts as a wrapper around an async server instance and initializes
    a :class:`ControlSession <asyncio_taskpool.control.session.ControlSession>`
    once a client connects to it.
    The interface is defined within the session class.
    """

    _client_class: type[ClientT]

    @classmethod  # type: ignore[misc]
    @property
    def client_class_name(cls) -> str:  # noqa
        """Returns the name of the matching control client class."""
        return cls._client_class.__name__

    def __init__(self, pool: AnyTaskPoolT, **server_kwargs: Any) -> None:
        """
        Merely sets internal attributes, but does not start the server yet.

        The task pool must be passed here and can not be set/changed afterwards.
        This means a control server is always tied to one specific task pool.

        Args:
            pool:
                An instance of a `BaseTaskPool` subclass to tie the server to.
            **server_kwargs (optional):
                Will be passed into the function that starts the server.
        """
        self._pool: AnyTaskPoolT = pool
        self._server_kwargs = server_kwargs
        self._server: AbstractServer | None = None

    @property
    def pool(self) -> AnyTaskPoolT:
        """The task pool instance controlled by the server."""
        return self._pool

    def is_serving(self) -> bool:
        """Wrapper around the `asyncio.Server.is_serving` method."""
        if self._server is None:
            return False
        return self._server.is_serving()

    async def _client_connected_cb(
        self,
        reader: StreamReader,
        writer: StreamWriter,
    ) -> None:
        """
        Universal client callback passed into the `_get_server_instance` method.

        Instantiates a control session, performs the client handshake, and
        enters the session's `listen` loop.
        """
        session = ControlSession(self, reader, writer)
        await session.client_handshake()
        await session.listen()

    @abstractmethod
    async def _get_server_instance(
        self,
        client_connected_cb: ConnectedCallbackT,
        **kwargs: Any,
    ) -> AbstractServer:
        """
        Initializes, starts, and returns an async server instance.

        Args:
            client_connected_cb:
                The callback for when a client connects to the server (as per
                `asyncio.start_server` or `asyncio.start_unix_server`);
                will always be the internal `_client_connected_cb` method.
            **kwargs (optional):
                Passed into the function that starts the server.

        Returns:
            The running server object (a type of `asyncio.Server`).
        """
        raise NotImplementedError

    @abstractmethod
    def _final_callback(self) -> None:
        """Runs after the server's `serve_forever` methods ends."""
        raise NotImplementedError

    async def _serve_forever(self) -> None:
        """
        To be run as an `asyncio.Task` by the following method.

        Serves as a wrapper around the the `asyncio.Server.serve_forever` method
        that ensures that the `_final_callback` method is called, when the
        former method ends for whatever reason.
        """
        if self._server is None:
            raise ServerNotInitialized
        try:
            async with self._server:
                await self._server.serve_forever()
        except CancelledError:
            log.debug("%s stopped", self.__class__.__name__)
        finally:
            self._final_callback()

    async def serve_forever(self) -> Task[None]:
        """
        Starts the server and begins listening to client connections.

        Should never block because serving will be performed in a separate task.

        Returns:
            The serving task. To stop the server, that task should be cancelled.
        """
        log.debug("Starting %s...", self.__class__.__name__)
        self._server = await self._get_server_instance(
            self._client_connected_cb, **self._server_kwargs
        )
        return create_task(self._serve_forever())


class TCPControlServer(ControlServer[TCPControlClient]):
    """Exposes a TCP socket for control clients to connect to."""

    _client_class = TCPControlClient

    def __init__(
        self,
        pool: AnyTaskPoolT,
        host: str,
        port: int | str,
        **server_kwargs: Any,
    ) -> None:
        """`host` and `port` are expected as non-optional server arguments."""
        self._host = host
        self._port = port
        super().__init__(pool, **server_kwargs)

    async def _get_server_instance(
        self,
        client_connected_cb: ConnectedCallbackT,
        **kwargs: Any,
    ) -> AbstractServer:
        server = await start_server(
            client_connected_cb, self._host, self._port, **kwargs
        )
        log.debug("Opened socket at %s:%s", self._host, self._port)
        return server

    def _final_callback(self) -> None:
        log.debug("Closed socket at %s:%s", self._host, self._port)


class UnixControlServer(ControlServer[UnixControlClient]):
    """Exposes a unix socket for control clients to connect to."""

    _client_class = UnixControlClient

    def __init__(
        self,
        pool: AnyTaskPoolT,
        socket_path: PathT,
        **server_kwargs: Any,
    ) -> None:
        """`socket_path` is expected as a non-optional server argument."""
        from asyncio.streams import start_unix_server

        self._start_unix_server = start_unix_server
        self._socket_path = Path(socket_path)
        super().__init__(pool, **server_kwargs)

    async def _get_server_instance(
        self,
        client_connected_cb: ConnectedCallbackT,
        **kwargs: Any,
    ) -> AbstractServer:
        server = await self._start_unix_server(
            client_connected_cb, self._socket_path, **kwargs
        )
        log.debug("Opened socket '%s'", str(self._socket_path))
        return server

    def _final_callback(self) -> None:
        """Removes the unix socket on which the server was listening."""
        self._socket_path.unlink()
        log.debug("Removed socket '%s'", str(self._socket_path))
