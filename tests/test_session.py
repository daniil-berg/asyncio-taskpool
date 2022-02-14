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
Unittests for the `asyncio_taskpool.session` module.
"""


import json
from argparse import ArgumentError, Namespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch, call

from asyncio_taskpool import session
from asyncio_taskpool.constants import CLIENT_INFO, CMD, SESSION_MSG_BYTES, SESSION_WRITER
from asyncio_taskpool.exceptions import HelpRequested, NotATaskPool, UnknownTaskPoolClass
from asyncio_taskpool.pool import BaseTaskPool, TaskPool, SimpleTaskPool


FOO, BAR = 'foo', 'bar'


class ControlServerTestCase(IsolatedAsyncioTestCase):
    log_lvl: int

    @classmethod
    def setUpClass(cls) -> None:
        cls.log_lvl = session.log.level
        session.log.setLevel(999)

    @classmethod
    def tearDownClass(cls) -> None:
        session.log.setLevel(cls.log_lvl)

    def setUp(self) -> None:
        self.mock_pool = MagicMock(spec=SimpleTaskPool(AsyncMock()))
        self.mock_client_class_name = FOO + BAR
        self.mock_server = MagicMock(pool=self.mock_pool,
                                     client_class_name=self.mock_client_class_name)
        self.mock_reader = MagicMock()
        self.mock_writer = MagicMock()
        self.session = session.ControlSession(self.mock_server, self.mock_reader, self.mock_writer)

    def test_init(self):
        self.assertEqual(self.mock_server, self.session._control_server)
        self.assertEqual(self.mock_pool, self.session._pool)
        self.assertEqual(self.mock_client_class_name, self.session._client_class_name)
        self.assertEqual(self.mock_reader, self.session._reader)
        self.assertEqual(self.mock_writer, self.session._writer)
        self.assertIsNone(self.session._parser)
        self.assertIsNone(self.session._subparsers)

    def test__add_command(self):
        expected_output = 123456
        mock_add_parser = MagicMock(return_value=expected_output)
        self.session._subparsers = MagicMock(add_parser=mock_add_parser)
        self.session._parser = MagicMock()
        name, prog, short_help, long_help = 'abc', None, 'short123', None
        kwargs = {'x': 1, 'y': 2}
        output = self.session._add_command(name, prog, short_help, long_help, **kwargs)
        self.assertEqual(expected_output, output)
        mock_add_parser.assert_called_once_with(name, prog=name, help=short_help, description=short_help,
                                                parent=self.session._parser, **kwargs)

        mock_add_parser.reset_mock()

        prog, long_help = 'ffffff', 'so long, wow'
        output = self.session._add_command(name, prog, short_help, long_help, **kwargs)
        self.assertEqual(expected_output, output)
        mock_add_parser.assert_called_once_with(name, prog=prog, help=short_help, description=long_help,
                                                parent=self.session._parser, **kwargs)

        mock_add_parser.reset_mock()

        short_help = None
        output = self.session._add_command(name, prog, short_help, long_help, **kwargs)
        self.assertEqual(expected_output, output)
        mock_add_parser.assert_called_once_with(name, prog=prog, help=long_help, description=long_help,
                                                parent=self.session._parser, **kwargs)

    @patch.object(session, 'get_first_doc_line')
    @patch.object(session.ControlSession, '_add_command')
    def test__adding_commands(self, mock__add_command: MagicMock, mock_get_first_doc_line: MagicMock):
        self.assertIsNone(self.session._add_base_commands())
        mock__add_command.assert_called()
        mock_get_first_doc_line.assert_called()

        mock__add_command.reset_mock()
        mock_get_first_doc_line.reset_mock()

        self.assertIsNone(self.session._add_simple_commands())
        mock__add_command.assert_called()
        mock_get_first_doc_line.assert_called()

        with self.assertRaises(NotImplementedError):
            self.session._add_advanced_commands()

    @patch.object(session.ControlSession, '_add_simple_commands')
    @patch.object(session.ControlSession, '_add_advanced_commands')
    @patch.object(session.ControlSession, '_add_base_commands')
    @patch.object(session, 'CommandParser')
    def test__init_parser(self, mock_command_parser_cls: MagicMock, mock__add_base_commands: MagicMock,
                          mock__add_advanced_commands: MagicMock, mock__add_simple_commands: MagicMock):
        mock_command_parser_cls.return_value = mock_parser = MagicMock()
        self.session._pool = TaskPool()
        width = 1234
        expected_parser_kwargs = {
            'prog': '',
            SESSION_WRITER: self.mock_writer,
            CLIENT_INFO.TERMINAL_WIDTH: width,
        }
        self.assertIsNone(self.session._init_parser(width))
        mock_command_parser_cls.assert_called_once_with(**expected_parser_kwargs)
        mock_parser.add_subparsers.assert_called_once_with(title="Commands", dest=CMD.CMD)
        mock__add_base_commands.assert_called_once_with()
        mock__add_advanced_commands.assert_called_once_with()
        mock__add_simple_commands.assert_not_called()

        mock_command_parser_cls.reset_mock()
        mock_parser.add_subparsers.reset_mock()
        mock__add_base_commands.reset_mock()
        mock__add_advanced_commands.reset_mock()
        mock__add_simple_commands.reset_mock()

        async def fake_coroutine(): pass

        self.session._pool = SimpleTaskPool(fake_coroutine)
        self.assertIsNone(self.session._init_parser(width))
        mock_command_parser_cls.assert_called_once_with(**expected_parser_kwargs)
        mock_parser.add_subparsers.assert_called_once_with(title="Commands", dest=CMD.CMD)
        mock__add_base_commands.assert_called_once_with()
        mock__add_advanced_commands.assert_not_called()
        mock__add_simple_commands.assert_called_once_with()

        mock_command_parser_cls.reset_mock()
        mock_parser.add_subparsers.reset_mock()
        mock__add_base_commands.reset_mock()
        mock__add_advanced_commands.reset_mock()
        mock__add_simple_commands.reset_mock()

        class FakeTaskPool(BaseTaskPool):
            pass

        self.session._pool = FakeTaskPool()
        with self.assertRaises(UnknownTaskPoolClass):
            self.session._init_parser(width)
        mock_command_parser_cls.assert_called_once_with(**expected_parser_kwargs)
        mock_parser.add_subparsers.assert_called_once_with(title="Commands", dest=CMD.CMD)
        mock__add_base_commands.assert_called_once_with()
        mock__add_advanced_commands.assert_not_called()
        mock__add_simple_commands.assert_not_called()

        mock_command_parser_cls.reset_mock()
        mock_parser.add_subparsers.reset_mock()
        mock__add_base_commands.reset_mock()
        mock__add_advanced_commands.reset_mock()
        mock__add_simple_commands.reset_mock()

        self.session._pool = MagicMock()
        with self.assertRaises(NotATaskPool):
            self.session._init_parser(width)
        mock_command_parser_cls.assert_called_once_with(**expected_parser_kwargs)
        mock_parser.add_subparsers.assert_called_once_with(title="Commands", dest=CMD.CMD)
        mock__add_base_commands.assert_called_once_with()
        mock__add_advanced_commands.assert_not_called()
        mock__add_simple_commands.assert_not_called()

    @patch.object(session.ControlSession, '_init_parser')
    async def test_client_handshake(self, mock__init_parser: MagicMock):
        width = 5678
        msg = ' ' + json.dumps({CLIENT_INFO.TERMINAL_WIDTH: width, FOO: BAR}) + '  '
        mock_read = AsyncMock(return_value=msg.encode())
        self.mock_reader.read = mock_read
        self.mock_writer.drain = AsyncMock()
        self.assertIsNone(await self.session.client_handshake())
        mock_read.assert_awaited_once_with(SESSION_MSG_BYTES)
        mock__init_parser.assert_called_once_with(width)
        self.mock_writer.write.assert_called_once_with(str(self.mock_pool).encode())
        self.mock_writer.drain.assert_awaited_once_with()

    @patch.object(session, 'return_or_exception')
    async def test__write_function_output(self, mock_return_or_exception: MagicMock):
        self.mock_writer.write = MagicMock()
        mock_return_or_exception.return_value = None
        func, args, kwargs = MagicMock(), (1, 2, 3), {'a': 'A', 'b': 'B'}
        self.assertIsNone(await self.session._write_function_output(func, *args, **kwargs))
        mock_return_or_exception.assert_called_once_with(func, *args, **kwargs)
        self.mock_writer.write.assert_called_once_with(b"ok")

        mock_return_or_exception.reset_mock()
        self.mock_writer.write.reset_mock()

        mock_return_or_exception.return_value = output = MagicMock()
        self.assertIsNone(await self.session._write_function_output(func, *args, **kwargs))
        mock_return_or_exception.assert_called_once_with(func, *args, **kwargs)
        self.mock_writer.write.assert_called_once_with(str(output).encode())

    @patch.object(session.ControlSession, '_write_function_output')
    async def test__cmd_name(self, mock__write_function_output: AsyncMock):
        self.assertIsNone(await self.session._cmd_name())
        mock__write_function_output.assert_awaited_once_with(self.mock_pool.__class__.__str__, self.session._pool)

    @patch.object(session.ControlSession, '_write_function_output')
    async def test__cmd_pool_size(self, mock__write_function_output: AsyncMock):
        num = 12345
        kwargs = {session.NUM: num, FOO: BAR}
        self.assertIsNone(await self.session._cmd_pool_size(**kwargs))
        mock__write_function_output.assert_awaited_once_with(
            self.mock_pool.__class__.pool_size.fset, self.session._pool, num
        )

        mock__write_function_output.reset_mock()

        kwargs.pop(session.NUM)
        self.assertIsNone(await self.session._cmd_pool_size(**kwargs))
        mock__write_function_output.assert_awaited_once_with(
            self.mock_pool.__class__.pool_size.fget, self.session._pool
        )

    @patch.object(session.ControlSession, '_write_function_output')
    async def test__cmd_num_running(self, mock__write_function_output: AsyncMock):
        self.assertIsNone(await self.session._cmd_num_running())
        mock__write_function_output.assert_awaited_once_with(
            self.mock_pool.__class__.num_running.fget, self.session._pool
        )

    @patch.object(session.ControlSession, '_write_function_output')
    async def test__cmd_start(self, mock__write_function_output: AsyncMock):
        num = 12345
        kwargs = {session.NUM: num, FOO: BAR}
        self.assertIsNone(await self.session._cmd_start(**kwargs))
        mock__write_function_output.assert_awaited_once_with(self.mock_pool.start, num)

    @patch.object(session.ControlSession, '_write_function_output')
    async def test__cmd_stop(self, mock__write_function_output: AsyncMock):
        num = 12345
        kwargs = {session.NUM: num, FOO: BAR}
        self.assertIsNone(await self.session._cmd_stop(**kwargs))
        mock__write_function_output.assert_awaited_once_with(self.mock_pool.stop, num)

    @patch.object(session.ControlSession, '_write_function_output')
    async def test__cmd_stop_all(self, mock__write_function_output: AsyncMock):
        self.assertIsNone(await self.session._cmd_stop_all())
        mock__write_function_output.assert_awaited_once_with(self.mock_pool.stop_all)

    @patch.object(session.ControlSession, '_write_function_output')
    async def test__cmd_func_name(self, mock__write_function_output: AsyncMock):
        self.assertIsNone(await self.session._cmd_func_name())
        mock__write_function_output.assert_awaited_once_with(
            self.mock_pool.__class__.func_name.fget, self.session._pool
        )

    async def test__execute_command(self):
        mock_method = AsyncMock()
        cmd = 'this-is-a-test'
        setattr(self.session, '_cmd_' + cmd.replace('-', '_'), mock_method)
        kwargs = {FOO: BAR, 'hello': 'python'}
        self.assertIsNone(await self.session._execute_command(**{CMD.CMD: cmd}, **kwargs))
        mock_method.assert_awaited_once_with(**kwargs)

    @patch.object(session.ControlSession, '_execute_command')
    async def test__parse_command(self, mock__execute_command: AsyncMock):
        msg = 'asdf asd as a'
        kwargs = {FOO: BAR, 'hello': 'python'}
        mock_parse_args = MagicMock(return_value=Namespace(**kwargs))
        self.session._parser = MagicMock(parse_args=mock_parse_args)
        self.mock_writer.write = MagicMock()
        self.assertIsNone(await self.session._parse_command(msg))
        mock_parse_args.assert_called_once_with(msg.split(' '))
        self.mock_writer.write.assert_not_called()
        mock__execute_command.assert_awaited_once_with(**kwargs)

        mock__execute_command.reset_mock()
        mock_parse_args.reset_mock()

        mock_parse_args.side_effect = exc = ArgumentError(MagicMock(), "oops")
        self.assertIsNone(await self.session._parse_command(msg))
        mock_parse_args.assert_called_once_with(msg.split(' '))
        self.mock_writer.write.assert_called_once_with(str(exc).encode())
        mock__execute_command.assert_not_awaited()

        self.mock_writer.write.reset_mock()
        mock_parse_args.reset_mock()

        mock_parse_args.side_effect = HelpRequested()
        self.assertIsNone(await self.session._parse_command(msg))
        mock_parse_args.assert_called_once_with(msg.split(' '))
        self.mock_writer.write.assert_not_called()
        mock__execute_command.assert_not_awaited()

    @patch.object(session.ControlSession, '_parse_command')
    async def test_listen(self, mock__parse_command: AsyncMock):
        def make_reader_return_empty():
            self.mock_reader.read.return_value = b''
        self.mock_writer.drain = AsyncMock(side_effect=make_reader_return_empty)
        msg = "fascinating"
        self.mock_reader.read = AsyncMock(return_value=f' {msg} '.encode())
        self.assertIsNone(await self.session.listen())
        self.mock_reader.read.assert_has_awaits([call(SESSION_MSG_BYTES), call(SESSION_MSG_BYTES)])
        mock__parse_command.assert_awaited_once_with(msg)
        self.mock_writer.drain.assert_awaited_once_with()

        self.mock_reader.read.reset_mock()
        mock__parse_command.reset_mock()
        self.mock_writer.drain.reset_mock()

        self.mock_server.is_serving = MagicMock(return_value=False)
        self.assertIsNone(await self.session.listen())
        self.mock_reader.read.assert_not_awaited()
        mock__parse_command.assert_not_awaited()
        self.mock_writer.drain.assert_not_awaited()
