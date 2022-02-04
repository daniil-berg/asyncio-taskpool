import logging
from abc import ABC, abstractmethod
from asyncio import AbstractServer
from asyncio.exceptions import CancelledError
from asyncio.streams import StreamReader, StreamWriter, start_unix_server
from asyncio.tasks import Task, create_task
from pathlib import Path
from typing import Tuple, Union, Optional

from . import constants
from .pool import TaskPool
from .client import ControlClient, UnixControlClient


log = logging.getLogger(__name__)


def tasks_str(num: int) -> str:
    return "tasks" if num != 1 else "task"


def get_cmd_arg(msg: str) -> Union[Tuple[str, Optional[int]], Tuple[None, None]]:
    cmd = msg.strip().split(' ', 1)
    if len(cmd) > 1:
        try:
            return cmd[0], int(cmd[1])
        except ValueError:
            return None, None
    return cmd[0], None


class ControlServer(ABC):
    client_class = ControlClient

    @abstractmethod
    async def get_server_instance(self, client_connected_cb, **kwargs) -> AbstractServer:
        raise NotImplementedError

    @abstractmethod
    def final_callback(self) -> None:
        raise NotImplementedError

    def __init__(self, pool: TaskPool, **server_kwargs) -> None:
        self._pool: TaskPool = pool
        self._server_kwargs = server_kwargs
        self._server: Optional[AbstractServer] = None

    def _start_tasks(self, num: int, writer: StreamWriter) -> None:
        log.debug("Client requests starting %s tasks", num)
        self._pool.start(num)
        size = self._pool.size
        writer.write(f"{num} new {tasks_str(num)} started! {size} {tasks_str(size)} active now.".encode())

    def _stop_tasks(self, num: int, writer: StreamWriter) -> None:
        log.debug("Client requests stopping %s tasks", num)
        num = self._pool.stop(num)  # the requested number may be greater than the total number of running tasks
        size = self._pool.size
        writer.write(f"{num} {tasks_str(num)} stopped! {size} {tasks_str(size)} left.".encode())

    def _stop_all_tasks(self, writer: StreamWriter) -> None:
        log.debug("Client requests stopping all tasks")
        num = self._pool.stop_all()
        writer.write(f"Remaining {num} {tasks_str(num)} stopped!".encode())

    async def _client_connected_cb(self, reader: StreamReader, writer: StreamWriter) -> None:
        log.debug("%s connected", self.client_class.__name__)
        writer.write(f"{self.__class__.__name__} for {self._pool}".encode())
        await writer.drain()
        while True:
            msg = (await reader.read(constants.MSG_BYTES)).decode().strip()
            if not msg:
                log.debug("%s disconnected", self.client_class.__name__)
                break
            cmd, arg = get_cmd_arg(msg)
            if cmd == constants.CMD_START:
                self._start_tasks(arg, writer)
            elif cmd == constants.CMD_STOP:
                self._stop_tasks(arg, writer)
            elif cmd == constants.CMD_STOP_ALL:
                self._stop_all_tasks(writer)
            else:
                log.debug("%s sent invalid command: %s", self.client_class.__name__, msg)
                writer.write(b"Invalid command!")
            await writer.drain()

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
    client_class = UnixControlClient

    def __init__(self, pool: TaskPool, **server_kwargs) -> None:
        self._socket_path = Path(server_kwargs.pop('path'))
        super().__init__(pool, **server_kwargs)

    async def get_server_instance(self, client_connected_cb, **kwargs) -> AbstractServer:
        srv = await start_unix_server(client_connected_cb, self._socket_path, **kwargs)
        log.debug("Opened socket '%s'", str(self._socket_path))
        return srv

    def final_callback(self) -> None:
        self._socket_path.unlink()
        log.debug("Removed socket '%s'", str(self._socket_path))
