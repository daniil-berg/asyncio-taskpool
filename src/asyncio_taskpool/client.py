import sys
from abc import ABC, abstractmethod
from asyncio.streams import StreamReader, StreamWriter, open_unix_connection
from pathlib import Path

from asyncio_taskpool import constants
from asyncio_taskpool.types import ClientConnT


class ControlClient(ABC):

    @abstractmethod
    async def open_connection(self, **kwargs) -> ClientConnT:
        raise NotImplementedError

    def __init__(self, **conn_kwargs) -> None:
        self._conn_kwargs = conn_kwargs
        self._connected: bool = False

    async def _interact(self, reader: StreamReader, writer: StreamWriter) -> None:
        try:
            msg = input("> ").strip().lower()
        except EOFError:
            msg = constants.CLIENT_EXIT
        if msg == constants.CLIENT_EXIT:
            writer.close()
            self._connected = False
            return
        writer.write(msg.encode())
        await writer.drain()
        print("Command sent; awaiting response...")
        print("Server response:", (await reader.read(constants.MSG_BYTES)).decode())

    async def start(self):
        reader, writer = await self.open_connection(**self._conn_kwargs)
        if reader is None:
            print("Failed to connect.", file=sys.stderr)
            return
        self._connected = True
        print("Connected to", (await reader.read(constants.MSG_BYTES)).decode())
        while self._connected:
            await self._interact(reader, writer)
        print("Disconnected from control server.")


class UnixControlClient(ControlClient):
    def __init__(self, **conn_kwargs) -> None:
        self._socket_path = Path(conn_kwargs.pop('path'))
        super().__init__(**conn_kwargs)

    async def open_connection(self, **kwargs) -> ClientConnT:
        try:
            return await open_unix_connection(self._socket_path, **kwargs)
        except FileNotFoundError:
            print("No socket at", self._socket_path, file=sys.stderr)
            return None, None
