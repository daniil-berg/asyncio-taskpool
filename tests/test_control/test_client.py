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
Unittests for the `asyncio_taskpool.client` module.
"""


import json
import os
import shutil
import sys
from pathlib import Path
from unittest import IsolatedAsyncioTestCase, skipIf
from unittest.mock import AsyncMock, MagicMock, call, patch

from asyncio_taskpool.control import client
from asyncio_taskpool.internals.constants import CLIENT_INFO, SESSION_MSG_BYTES


FOO, BAR = 'foo', 'bar'


class ControlClientTestCase(IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.abstract_patcher = patch('asyncio_taskpool.control.client.ControlClient.__abstractmethods__', set())
        self.print_patcher = patch.object(client, 'print')
        self.mock_abstract_methods = self.abstract_patcher.start()
        self.mock_print = self.print_patcher.start()
        self.kwargs = {FOO: 123, BAR: 456}
        self.client = client.ControlClient(**self.kwargs)

        self.mock_read = AsyncMock(return_value=FOO.encode())
        self.mock_write, self.mock_drain = MagicMock(), AsyncMock()
        self.mock_reader = MagicMock(read=self.mock_read)
        self.mock_writer = MagicMock(write=self.mock_write, drain=self.mock_drain)

    def tearDown(self) -> None:
        self.abstract_patcher.stop()
        self.print_patcher.stop()

    def test_client_info(self):
        self.assertEqual({CLIENT_INFO.TERMINAL_WIDTH: shutil.get_terminal_size().columns},
                         client.ControlClient._client_info())

    async def test_abstract(self):
        with self.assertRaises(NotImplementedError):
            await self.client._open_connection(**self.kwargs)

    def test_init(self):
        self.assertEqual(self.kwargs, self.client._conn_kwargs)
        self.assertFalse(self.client._connected)

    @patch.object(client.ControlClient, '_client_info')
    async def test__server_handshake(self, mock__client_info: MagicMock):
        mock__client_info.return_value = mock_info = {FOO: 1, BAR: 9999}
        self.assertIsNone(await self.client._server_handshake(self.mock_reader, self.mock_writer))
        self.assertTrue(self.client._connected)
        mock__client_info.assert_called_once_with()
        self.mock_write.assert_called_once_with(json.dumps(mock_info).encode())
        self.mock_drain.assert_awaited_once_with()
        self.mock_read.assert_awaited_once_with(SESSION_MSG_BYTES)
        self.mock_print.assert_has_calls([
            call("Connected to", self.mock_read.return_value.decode()),
            call("Type '-h' to get help and usage instructions for all available commands.\n")
        ])

    @patch.object(client, 'input')
    def test__get_command(self, mock_input: MagicMock):
        self.client._connected = True

        mock_input.return_value = ' ' + FOO.upper() + ' '
        mock_close = MagicMock()
        mock_writer = MagicMock(close=mock_close)
        output = self.client._get_command(mock_writer)
        self.assertEqual(FOO, output)
        mock_input.assert_called_once()
        mock_close.assert_not_called()
        self.assertTrue(self.client._connected)

        mock_input.reset_mock()
        mock_input.side_effect = KeyboardInterrupt
        self.assertIsNone(self.client._get_command(mock_writer))
        mock_input.assert_called_once()
        mock_close.assert_not_called()
        self.assertTrue(self.client._connected)

        mock_input.reset_mock()
        mock_input.side_effect = EOFError
        self.assertIsNone(self.client._get_command(mock_writer))
        mock_input.assert_called_once()
        mock_close.assert_called_once()
        self.assertFalse(self.client._connected)

    @patch.object(client.ControlClient, '_get_command')
    async def test__interact(self, mock__get_command: MagicMock):
        self.client._connected = True

        mock__get_command.return_value = None
        self.assertIsNone(await self.client._interact(self.mock_reader, self.mock_writer))
        self.mock_write.assert_not_called()
        self.mock_drain.assert_not_awaited()
        self.mock_read.assert_not_awaited()
        self.mock_print.assert_not_called()
        self.assertTrue(self.client._connected)

        mock__get_command.return_value = cmd = FOO + BAR + ' 123'
        self.mock_drain.side_effect = err = ConnectionError()
        self.assertIsNone(await self.client._interact(self.mock_reader, self.mock_writer))
        self.mock_write.assert_called_once_with(cmd.encode())
        self.mock_drain.assert_awaited_once_with()
        self.mock_read.assert_not_awaited()
        self.mock_print.assert_called_once_with(err, file=sys.stderr)
        self.assertFalse(self.client._connected)

        self.client._connected = True
        self.mock_write.reset_mock()
        self.mock_drain.reset_mock(side_effect=True)
        self.mock_print.reset_mock()

        self.assertIsNone(await self.client._interact(self.mock_reader, self.mock_writer))
        self.mock_write.assert_called_once_with(cmd.encode())
        self.mock_drain.assert_awaited_once_with()
        self.mock_read.assert_awaited_once_with(SESSION_MSG_BYTES)
        self.mock_print.assert_called_once_with(FOO)
        self.assertTrue(self.client._connected)

    @patch.object(client.ControlClient, '_interact')
    @patch.object(client.ControlClient, '_server_handshake')
    @patch.object(client.ControlClient, '_open_connection')
    async def test_start(self, mock__open_connection: AsyncMock, mock__server_handshake: AsyncMock,
                         mock__interact: AsyncMock):
        mock__open_connection.return_value = None, None
        self.assertIsNone(await self.client.start())
        mock__open_connection.assert_awaited_once_with(**self.kwargs)
        mock__server_handshake.assert_not_awaited()
        mock__interact.assert_not_awaited()
        self.mock_print.assert_called_once_with("Failed to connect.", file=sys.stderr)

        mock__open_connection.reset_mock()
        self.mock_print.reset_mock()

        mock__open_connection.return_value = self.mock_reader, self.mock_writer
        self.assertIsNone(await self.client.start())
        mock__open_connection.assert_awaited_once_with(**self.kwargs)
        mock__server_handshake.assert_awaited_once_with(self.mock_reader, self.mock_writer)
        mock__interact.assert_not_awaited()
        self.mock_print.assert_called_once_with("Disconnected from control server.")

        mock__open_connection.reset_mock()
        mock__server_handshake.reset_mock()
        self.mock_print.reset_mock()

        self.client._connected = True
        def disconnect(*_args, **_kwargs) -> None: self.client._connected = False
        mock__interact.side_effect = disconnect
        self.assertIsNone(await self.client.start())
        mock__open_connection.assert_awaited_once_with(**self.kwargs)
        mock__server_handshake.assert_awaited_once_with(self.mock_reader, self.mock_writer)
        mock__interact.assert_awaited_once_with(self.mock_reader, self.mock_writer)
        self.mock_print.assert_called_once_with("Disconnected from control server.")


class TCPControlClientTestCase(IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.base_init_patcher = patch.object(client.ControlClient, '__init__')
        self.mock_base_init = self.base_init_patcher.start()
        self.host, self.port = 'localhost', 12345
        self.kwargs = {FOO: 123, BAR: 456}
        self.client = client.TCPControlClient(host=self.host, port=self.port, **self.kwargs)

    def tearDown(self) -> None:
        self.base_init_patcher.stop()

    def test_init(self):
        self.assertEqual(self.host, self.client._host)
        self.assertEqual(self.port, self.client._port)
        self.mock_base_init.assert_called_once_with(**self.kwargs)

    @patch.object(client, 'print')
    @patch.object(client, 'open_connection')
    async def test__open_connection(self, mock_open_connection: AsyncMock, mock_print: MagicMock):
        mock_open_connection.return_value = expected_output = 'something'
        kwargs = {'a': 1, 'b': 2}
        output = await self.client._open_connection(**kwargs)
        self.assertEqual(expected_output, output)
        mock_open_connection.assert_awaited_once_with(self.host, self.port, **kwargs)
        mock_print.assert_not_called()

        mock_open_connection.reset_mock()

        mock_open_connection.side_effect = e = ConnectionError()
        output1, output2 = await self.client._open_connection(**kwargs)
        self.assertIsNone(output1)
        self.assertIsNone(output2)
        mock_open_connection.assert_awaited_once_with(self.host, self.port, **kwargs)
        mock_print.assert_called_once_with(str(e), file=sys.stderr)


@skipIf(os.name == 'nt', "No Unix sockets on Windows :(")
class UnixControlClientTestCase(IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.base_init_patcher = patch.object(client.ControlClient, '__init__')
        self.mock_base_init = self.base_init_patcher.start()
        self.path = '/tmp/asyncio_taskpool'
        self.kwargs = {FOO: 123, BAR: 456}
        self.client = client.UnixControlClient(socket_path=self.path, **self.kwargs)

    def tearDown(self) -> None:
        self.base_init_patcher.stop()

    def test_init(self):
        self.assertEqual(Path(self.path), self.client._socket_path)
        self.mock_base_init.assert_called_once_with(**self.kwargs)

    @patch.object(client, 'print')
    async def test__open_connection(self, mock_print: MagicMock):
        expected_output = 'something'
        self.client._open_unix_connection = mock_open_unix_connection = AsyncMock(return_value=expected_output)
        kwargs = {'a': 1, 'b': 2}
        output = await self.client._open_connection(**kwargs)
        self.assertEqual(expected_output, output)
        mock_open_unix_connection.assert_awaited_once_with(Path(self.path), **kwargs)
        mock_print.assert_not_called()

        mock_open_unix_connection.reset_mock()

        mock_open_unix_connection.side_effect = FileNotFoundError
        output1, output2 = await self.client._open_connection(**kwargs)
        self.assertIsNone(output1)
        self.assertIsNone(output2)
        mock_open_unix_connection.assert_awaited_once_with(Path(self.path), **kwargs)
        mock_print.assert_called_once_with("No socket at", Path(self.path), file=sys.stderr)
