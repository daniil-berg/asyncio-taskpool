import asyncio
import logging
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from asyncio_taskpool import server
from asyncio_taskpool.client import ControlClient, UnixControlClient


FOO, BAR = 'foo', 'bar'


class ControlServerTestCase(IsolatedAsyncioTestCase):
    log_lvl: int

    @classmethod
    def setUpClass(cls) -> None:
        cls.log_lvl = server.log.level
        server.log.setLevel(999)

    @classmethod
    def tearDownClass(cls) -> None:
        server.log.setLevel(cls.log_lvl)

    def setUp(self) -> None:
        self.abstract_patcher = patch('asyncio_taskpool.server.ControlServer.__abstractmethods__', set())
        self.mock_abstract_methods = self.abstract_patcher.start()
        self.mock_pool = MagicMock()
        self.kwargs = {FOO: 123, BAR: 456}
        self.server = server.ControlServer(pool=self.mock_pool, **self.kwargs)

    def tearDown(self) -> None:
        self.abstract_patcher.stop()

    def test_client_class_name(self):
        self.assertEqual(ControlClient.__name__, server.ControlServer.client_class_name)

    async def test_abstract(self):
        with self.assertRaises(NotImplementedError):
            args = [AsyncMock()]
            await self.server._get_server_instance(*args)
        with self.assertRaises(NotImplementedError):
            self.server._final_callback()

    def test_init(self):
        self.assertEqual(self.mock_pool, self.server._pool)
        self.assertEqual(self.kwargs, self.server._server_kwargs)
        self.assertIsNone(self.server._server)

    def test_pool(self):
        self.assertEqual(self.mock_pool, self.server.pool)

    def test_is_serving(self):
        self.server._server = MagicMock(is_serving=MagicMock(return_value=FOO + BAR))
        self.assertEqual(FOO + BAR, self.server.is_serving())

    @patch.object(server, 'ControlSession')
    async def test__client_connected_cb(self, mock_client_session_cls: MagicMock):
        mock_client_handshake, mock_listen = AsyncMock(), AsyncMock()
        mock_client_session_cls.return_value = MagicMock(client_handshake=mock_client_handshake, listen=mock_listen)
        mock_reader, mock_writer = MagicMock(), MagicMock()
        self.assertIsNone(await self.server._client_connected_cb(mock_reader, mock_writer))
        mock_client_session_cls.assert_called_once_with(self.server, mock_reader, mock_writer)
        mock_client_handshake.assert_awaited_once_with()
        mock_listen.assert_awaited_once_with()

    @patch.object(server.ControlServer, '_final_callback')
    async def test__serve_forever(self, mock__final_callback: MagicMock):
        mock_aenter, mock_serve_forever = AsyncMock(), AsyncMock(side_effect=asyncio.CancelledError)
        self.server._server = MagicMock(__aenter__=mock_aenter, serve_forever=mock_serve_forever)
        with self.assertLogs(server.log, logging.DEBUG):
            self.assertIsNone(await self.server._serve_forever())
        mock_aenter.assert_awaited_once_with()
        mock_serve_forever.assert_awaited_once_with()
        mock__final_callback.assert_called_once_with()

        mock_aenter.reset_mock()
        mock_serve_forever.reset_mock(side_effect=True)
        mock__final_callback.reset_mock()

        self.assertIsNone(await self.server._serve_forever())
        mock_aenter.assert_awaited_once_with()
        mock_serve_forever.assert_awaited_once_with()
        mock__final_callback.assert_called_once_with()

    @patch.object(server, 'create_task')
    @patch.object(server.ControlServer, '_serve_forever', new_callable=MagicMock())
    @patch.object(server.ControlServer, '_get_server_instance')
    async def test_serve_forever(self, mock__get_server_instance: AsyncMock, mock__serve_forever: MagicMock,
                                 mock_create_task: MagicMock):
        mock__serve_forever.return_value = mock_awaitable = 'some_coroutine'
        mock_create_task.return_value = expected_output = 12345
        output = await self.server.serve_forever()
        self.assertEqual(expected_output, output)
        mock__get_server_instance.assert_awaited_once_with(self.server._client_connected_cb, **self.kwargs)
        mock__serve_forever.assert_called_once_with()
        mock_create_task.assert_called_once_with(mock_awaitable)


class UnixControlServerTestCase(IsolatedAsyncioTestCase):
    log_lvl: int

    @classmethod
    def setUpClass(cls) -> None:
        cls.log_lvl = server.log.level
        server.log.setLevel(999)

    @classmethod
    def tearDownClass(cls) -> None:
        server.log.setLevel(cls.log_lvl)

    def setUp(self) -> None:
        self.base_init_patcher = patch.object(server.ControlServer, '__init__')
        self.mock_base_init = self.base_init_patcher.start()
        self.mock_pool = MagicMock()
        self.path = '/tmp/asyncio_taskpool'
        self.kwargs = {FOO: 123, BAR: 456}
        self.server = server.UnixControlServer(pool=self.mock_pool, path=self.path, **self.kwargs)

    def tearDown(self) -> None:
        self.base_init_patcher.stop()

    def test__client_class(self):
        self.assertEqual(UnixControlClient, self.server._client_class)

    def test_init(self):
        self.assertEqual(Path(self.path), self.server._socket_path)
        self.mock_base_init.assert_called_once_with(self.mock_pool, **self.kwargs)

    @patch.object(server, 'start_unix_server')
    async def test__get_server_instance(self, mock_start_unix_server: AsyncMock):
        mock_start_unix_server.return_value = expected_output = 'totally_a_server'
        mock_callback, mock_kwargs = MagicMock(), {'a': 1, 'b': 2}
        args = [mock_callback]
        output = await self.server._get_server_instance(*args, **mock_kwargs)
        self.assertEqual(expected_output, output)
        mock_start_unix_server.assert_called_once_with(mock_callback, Path(self.path), **mock_kwargs)

    def test__final_callback(self):
        self.server._socket_path = MagicMock()
        self.assertIsNone(self.server._final_callback())
        self.server._socket_path.unlink.assert_called_once_with()
