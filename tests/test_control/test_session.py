"""Unittests for the `asyncio_taskpool.session` module."""

from __future__ import annotations

import json
from argparse import ArgumentError, Namespace
from io import StringIO
from typing import Any
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, call, patch

from asyncio_taskpool.control import session
from asyncio_taskpool.exceptions import HelpRequested, ParserNotInitialized
from asyncio_taskpool.internals.constants import CLIENT_INFO, CMD, CMD_OK
from asyncio_taskpool.pool import SimpleTaskPool

FOO, BAR = "foo", "bar"


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
        self.mock_server = MagicMock(
            pool=self.mock_pool, client_class_name=self.mock_client_class_name
        )
        self.mock_reader = MagicMock()
        self.mock_writer = MagicMock()
        self.session = session.ControlSession(
            self.mock_server, self.mock_reader, self.mock_writer
        )

    def test_init(self) -> None:
        self.assertEqual(self.mock_server, self.session._control_server)
        self.assertEqual(self.mock_pool, self.session._pool)
        self.assertEqual(
            self.mock_client_class_name, self.session._client_class_name
        )
        self.assertEqual(self.mock_reader, self.session._reader)
        self.assertEqual(self.mock_writer, self.session._writer)
        self.assertIsNone(self.session._parser)
        self.assertIsInstance(self.session._response_buffer, StringIO)

    @patch.object(session, "return_or_exception")
    async def test__exec_method_and_respond(
        self, mock_return_or_exception: AsyncMock
    ) -> None:
        def method(
            self: Any, arg1: Any, arg2: Any, *var_args: Any, **rest: Any  # noqa
        ) -> None:
            pass

        test_arg1, test_arg2, test_var_args, test_rest = (
            123,
            "xyz",
            [0.1, 0.2, 0.3],
            {"aaa": 1, "bbb": 11},
        )
        kwargs = {
            "arg1": test_arg1,
            "arg2": test_arg2,
            "var_args": test_var_args,
        }
        mock_return_or_exception.return_value = None
        await self.session._exec_method_and_respond(
            method, **kwargs, **test_rest
        )
        mock_return_or_exception.assert_awaited_once_with(
            method,
            self.mock_pool,
            test_arg1,
            test_arg2,
            *test_var_args,
            **test_rest,
        )
        self.assertEqual(
            CMD_OK.decode(), self.session._response_buffer.getvalue()
        )

    @patch.object(session, "return_or_exception")
    async def test__exec_property_and_respond(
        self, mock_return_or_exception: AsyncMock
    ) -> None:
        def prop_get(_: Any) -> None:
            pass

        def prop_set(_: Any, __: Any) -> None:
            pass

        with self.assertRaises(TypeError):
            await self.session._exec_property_and_respond(property(), foo=1)
        with self.assertRaises(TypeError):
            await self.session._exec_property_and_respond(property())

        prop = property(prop_get, prop_set)
        kwargs = {"value": "something"}
        mock_return_or_exception.return_value = None
        await self.session._exec_property_and_respond(prop, **kwargs)
        mock_return_or_exception.assert_awaited_once_with(
            prop_set, self.mock_pool, **kwargs
        )
        self.assertEqual(
            CMD_OK.decode(), self.session._response_buffer.getvalue()
        )

        mock_return_or_exception.reset_mock()
        self.session._response_buffer.seek(0)
        self.session._response_buffer.truncate()

        mock_return_or_exception.return_value = val = 420.69
        await self.session._exec_property_and_respond(prop)
        mock_return_or_exception.assert_awaited_once_with(
            prop_get, self.mock_pool
        )
        self.assertEqual(str(val), self.session._response_buffer.getvalue())

    @patch.object(session, "ControlParser")
    async def test_client_handshake(self, mock_parser_cls: MagicMock) -> None:
        mock_add_subparsers, mock_add_class_commands = MagicMock(), MagicMock()
        mock_parser = MagicMock(
            add_subparsers=mock_add_subparsers,
            add_class_commands=mock_add_class_commands,
        )
        mock_parser_cls.return_value = mock_parser
        width = 5678
        msg = (
            " "
            + json.dumps({CLIENT_INFO.TERMINAL_WIDTH: width, FOO: BAR})
            + "  "
        )
        mock_readline = AsyncMock(return_value=msg.encode())
        self.mock_reader.readline = mock_readline
        self.mock_writer.drain = AsyncMock()
        expected_parser_kwargs = {
            "stream": self.session._response_buffer,
            CLIENT_INFO.TERMINAL_WIDTH: width,
            "prog": "",
            "usage": f"[-h] [{CMD}] ...",
        }
        expected_subparsers_kwargs = {
            "title": "Commands",
            "metavar": "(A command followed by '-h' or '--help' will show command-specific help.)",
        }
        await self.session.client_handshake()
        self.assertEqual(mock_parser, self.session._parser)
        mock_readline.assert_awaited_once_with()
        mock_parser_cls.assert_called_once_with(**expected_parser_kwargs)
        mock_add_subparsers.assert_called_once_with(
            **expected_subparsers_kwargs
        )
        mock_add_class_commands.assert_called_once_with(
            self.mock_pool.__class__
        )
        self.mock_writer.write.assert_called_once_with(
            str(self.mock_pool).encode() + b"\n"
        )
        self.mock_writer.drain.assert_awaited_once_with()

    @patch.object(session.ControlSession, "_exec_property_and_respond")
    @patch.object(session.ControlSession, "_exec_method_and_respond")
    async def test__parse_command(
        self,
        mock__exec_method_and_respond: AsyncMock,
        mock__exec_property_and_respond: AsyncMock,
    ) -> None:
        with self.assertRaises(ParserNotInitialized):
            await self.session._parse_command("...")

        def method(_: Any) -> None:
            pass

        prop = property(method)
        msg = "asdf asd as a"
        kwargs = {FOO: BAR, "hello": "python"}
        mock_parse_args = MagicMock(
            return_value=Namespace(**{CMD: method}, **kwargs)
        )
        self.session._parser = MagicMock(parse_args=mock_parse_args)
        await self.session._parse_command(msg)
        mock_parse_args.assert_called_once_with(msg.split(" "))
        self.assertEqual("", self.session._response_buffer.getvalue())
        mock__exec_method_and_respond.assert_awaited_once_with(method, **kwargs)
        mock__exec_property_and_respond.assert_not_called()

        mock__exec_method_and_respond.reset_mock()
        mock_parse_args.reset_mock()

        mock_parse_args.return_value = Namespace(**{CMD: prop}, **kwargs)
        await self.session._parse_command(msg)
        mock_parse_args.assert_called_once_with(msg.split(" "))
        self.assertEqual("", self.session._response_buffer.getvalue())
        mock__exec_method_and_respond.assert_not_called()
        mock__exec_property_and_respond.assert_awaited_once_with(prop, **kwargs)

        mock__exec_property_and_respond.reset_mock()
        mock_parse_args.reset_mock()

        bad_command = "definitely not a function or property"
        mock_parse_args.return_value = Namespace(**{CMD: bad_command}, **kwargs)
        exc: ArgumentError | MagicMock = MagicMock()
        with patch.object(session, "CommandError") as cmd_err_cls:
            cmd_err_cls.return_value = exc
            await self.session._parse_command(msg)
            cmd_err_cls.assert_called_once_with(
                f"Unknown command object: {bad_command}"
            )
        mock_parse_args.assert_called_once_with(msg.split(" "))
        mock__exec_method_and_respond.assert_not_called()
        mock__exec_property_and_respond.assert_not_called()
        self.assertEqual(str(exc), self.session._response_buffer.getvalue())

        mock__exec_property_and_respond.reset_mock()
        mock_parse_args.reset_mock()
        self.session._response_buffer.seek(0)
        self.session._response_buffer.truncate()

        mock_parse_args.side_effect = exc = ArgumentError(MagicMock(), "oops")
        await self.session._parse_command(msg)
        mock_parse_args.assert_called_once_with(msg.split(" "))
        self.assertEqual(str(exc), self.session._response_buffer.getvalue())
        mock__exec_method_and_respond.assert_not_awaited()
        mock__exec_property_and_respond.assert_not_awaited()

        mock_parse_args.reset_mock()
        self.session._response_buffer.seek(0)
        self.session._response_buffer.truncate()

        mock_parse_args.side_effect = HelpRequested()
        await self.session._parse_command(msg)
        mock_parse_args.assert_called_once_with(msg.split(" "))
        self.assertEqual("", self.session._response_buffer.getvalue())
        mock__exec_method_and_respond.assert_not_awaited()
        mock__exec_property_and_respond.assert_not_awaited()

    @patch.object(session.ControlSession, "_parse_command")
    async def test_listen(self, mock__parse_command: AsyncMock) -> None:
        def make_reader_return_empty() -> None:
            self.mock_reader.readline.return_value = b""

        self.mock_writer.drain = AsyncMock(side_effect=make_reader_return_empty)
        msg = "fascinating"
        self.mock_reader.readline = AsyncMock(return_value=f" {msg} ".encode())
        response = FOO + BAR + FOO
        self.session._response_buffer.write(response)
        await self.session.listen()
        self.mock_reader.readline.assert_has_awaits([call(), call()])
        mock__parse_command.assert_awaited_once_with(msg)
        self.assertEqual("", self.session._response_buffer.getvalue())
        self.mock_writer.write.assert_called_once_with(
            response.encode() + b"\n"
        )
        self.mock_writer.drain.assert_awaited_once_with()

        self.mock_reader.readline.reset_mock()
        mock__parse_command.reset_mock()
        self.mock_writer.write.reset_mock()
        self.mock_writer.drain.reset_mock()

        self.mock_server.is_serving = MagicMock(return_value=False)
        await self.session.listen()
        self.mock_reader.readline.assert_not_awaited()
        mock__parse_command.assert_not_awaited()
        self.mock_writer.write.assert_not_called()
        self.mock_writer.drain.assert_not_awaited()
