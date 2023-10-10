from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from asyncio_taskpool.control import __main__ as module
from asyncio_taskpool.control.client import TCPControlClient, UnixControlClient


class CLITestCase(IsolatedAsyncioTestCase):
    def test_parse_cli(self) -> None:
        socket_path = "/some/path/to.sock"
        args = [module.UNIX, socket_path]
        expected_kwargs = {
            module.CLIENT_CLASS: UnixControlClient,
            module.SOCKET_PATH: Path(socket_path),
        }
        parsed_kwargs = module.parse_cli(args)
        self.assertDictEqual(expected_kwargs, parsed_kwargs)

        host, port = "1.2.3.4", "1234"
        args = [module.TCP, host, port]
        expected_kwargs = {
            module.CLIENT_CLASS: TCPControlClient,
            module.HOST: host,
            module.PORT: int(port),
        }
        parsed_kwargs = module.parse_cli(args)
        self.assertDictEqual(expected_kwargs, parsed_kwargs)

        with patch("sys.stderr"):
            with self.assertRaises(SystemExit):
                module.parse_cli(["invalid", "foo", "bar"])

    @patch.object(module, "parse_cli")
    async def test_main(self, mock_parse_cli: MagicMock) -> None:
        mock_client_start = AsyncMock()
        mock_client = MagicMock(start=mock_client_start)
        mock_client_cls = MagicMock(return_value=mock_client)
        mock_client_kwargs = {"foo": 123, "bar": 456, "baz": 789}
        mock_parse_cli.return_value = {
            module.CLIENT_CLASS: mock_client_cls,
            **mock_client_kwargs,
        }
        await module.main()
        mock_parse_cli.assert_called_once_with()
        mock_client_cls.assert_called_once_with(**mock_client_kwargs)
        mock_client_start.assert_awaited_once_with()
