import logging
from abc import ABC, abstractmethod
from asyncio import AbstractServer
from asyncio.exceptions import CancelledError
from asyncio.streams import StreamReader, StreamWriter, start_unix_server
from asyncio.tasks import Task, create_task
from pathlib import Path
from typing import Tuple, Union, Optional

from . import constants
from .pool import SimpleTaskPool
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


class ControlServer(ABC):  # TODO: Implement interface for normal TaskPool instances, not just SimpleTaskPool
    client_class = ControlClient

    @abstractmethod
    async def get_server_instance(self, client_connected_cb, **kwargs) -> AbstractServer:
        raise NotImplementedError

    @abstractmethod
    def final_callback(self) -> None:
        raise NotImplementedError

    def __init__(self, pool: SimpleTaskPool, **server_kwargs) -> None:
        self._pool: SimpleTaskPool = pool
        self._server_kwargs = server_kwargs
        self._server: Optional[AbstractServer] = None

    async def _start_tasks(self, writer: StreamWriter, num: int = None) -> None:
        if num is None:
            num = 1
        log.debug("%s requests starting %s %s", self.client_class.__name__, num, tasks_str(num))
        writer.write(str(await self._pool.start(num)).encode())

    def _stop_tasks(self, writer: StreamWriter, num: int = None) -> None:
        if num is None:
            num = 1
        log.debug("%s requests stopping %s %s", self.client_class.__name__, num, tasks_str(num))
        # the requested number may be greater than the total number of running tasks
        writer.write(str(self._pool.stop(num)).encode())

    def _stop_all_tasks(self, writer: StreamWriter) -> None:
        log.debug("%s requests stopping all tasks", self.client_class.__name__)
        writer.write(str(self._pool.stop_all()).encode())

    def _pool_size(self, writer: StreamWriter) -> None:
        log.debug("%s requests number of running tasks", self.client_class.__name__)
        writer.write(str(self._pool.num_running).encode())

    def _pool_func(self, writer: StreamWriter) -> None:
        log.debug("%s requests pool function", self.client_class.__name__)
        writer.write(self._pool.func_name.encode())

    async def _listen(self, reader: StreamReader, writer: StreamWriter) -> None:
        while self._server.is_serving():
            msg = (await reader.read(constants.MSG_BYTES)).decode().strip()
            if not msg:
                log.debug("%s disconnected", self.client_class.__name__)
                break
            cmd, arg = get_cmd_arg(msg)
            if cmd == constants.CMD_START:
                await self._start_tasks(writer, arg)
            elif cmd == constants.CMD_STOP:
                self._stop_tasks(writer, arg)
            elif cmd == constants.CMD_STOP_ALL:
                self._stop_all_tasks(writer)
            elif cmd == constants.CMD_NUM_RUNNING:
                self._pool_size(writer)
            elif cmd == constants.CMD_FUNC:
                self._pool_func(writer)
            else:
                log.debug("%s sent invalid command: %s", self.client_class.__name__, msg)
                writer.write(b"Invalid command!")
            await writer.drain()

    async def _client_connected_cb(self, reader: StreamReader, writer: StreamWriter) -> None:
        log.debug("%s connected", self.client_class.__name__)
        writer.write(str(self._pool).encode())
        await writer.drain()
        await self._listen(reader, writer)

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
