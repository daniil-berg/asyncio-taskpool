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
Unittests for the `asyncio_taskpool.session_parser` module.
"""


from argparse import Action, ArgumentParser, HelpFormatter, ArgumentDefaultsHelpFormatter, RawTextHelpFormatter
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from asyncio_taskpool import session_parser
from asyncio_taskpool.constants import SESSION_WRITER, CLIENT_INFO
from asyncio_taskpool.exceptions import HelpRequested


FOO = 'foo'


class ControlServerTestCase(IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.help_formatter_factory_patcher = patch.object(session_parser.CommandParser, 'help_formatter_factory')
        self.mock_help_formatter_factory = self.help_formatter_factory_patcher.start()
        self.mock_help_formatter_factory.return_value = RawTextHelpFormatter
        self.session_writer, self.terminal_width = MagicMock(), 420
        self.kwargs = {
            SESSION_WRITER: self.session_writer,
            CLIENT_INFO.TERMINAL_WIDTH: self.terminal_width,
            session_parser.FORMATTER_CLASS: FOO
        }
        self.parser = session_parser.CommandParser(**self.kwargs)

    def tearDown(self) -> None:
        self.help_formatter_factory_patcher.stop()

    def test_help_formatter_factory(self):
        self.help_formatter_factory_patcher.stop()

        class MockBaseClass(HelpFormatter):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)

        terminal_width = 123456789
        cls = session_parser.CommandParser.help_formatter_factory(terminal_width, MockBaseClass)
        self.assertTrue(issubclass(cls, MockBaseClass))
        instance = cls('prog')
        self.assertEqual(terminal_width, getattr(instance, '_width'))

        cls = session_parser.CommandParser.help_formatter_factory(terminal_width)
        self.assertTrue(issubclass(cls, ArgumentDefaultsHelpFormatter))
        instance = cls('prog')
        self.assertEqual(terminal_width, getattr(instance, '_width'))

    def test_init(self):
        self.assertIsInstance(self.parser, ArgumentParser)
        self.assertEqual(self.session_writer, self.parser._session_writer)
        self.assertEqual(self.terminal_width, self.parser._terminal_width)
        self.mock_help_formatter_factory.assert_called_once_with(self.terminal_width, FOO)
        self.assertFalse(getattr(self.parser, 'exit_on_error'))
        self.assertEqual(RawTextHelpFormatter, getattr(self.parser, 'formatter_class'))

    def test_session_writer(self):
        self.assertEqual(self.session_writer, self.parser.session_writer)

    def test_terminal_width(self):
        self.assertEqual(self.terminal_width, self.parser.terminal_width)

    def test__print_message(self):
        self.session_writer.write = MagicMock()
        self.assertIsNone(self.parser._print_message(''))
        self.session_writer.write.assert_not_called()
        msg = 'foo bar baz'
        self.assertIsNone(self.parser._print_message(msg))
        self.session_writer.write.assert_called_once_with(msg.encode())

    @patch.object(session_parser.CommandParser, '_print_message')
    def test_exit(self, mock__print_message: MagicMock):
        self.assertIsNone(self.parser.exit(123, ''))
        mock__print_message.assert_not_called()
        msg = 'foo bar baz'
        self.assertIsNone(self.parser.exit(123, msg))
        mock__print_message.assert_called_once_with(msg)

    @patch.object(session_parser.ArgumentParser, 'print_help')
    def test_print_help(self, mock_print_help: MagicMock):
        arg = MagicMock()
        with self.assertRaises(HelpRequested):
            self.parser.print_help(arg)
        mock_print_help.assert_called_once_with(arg)

    def test_add_optional_num_argument(self):
        metavar = 'FOOBAR'
        action = self.parser.add_optional_num_argument(metavar=metavar)
        self.assertIsInstance(action, Action)
        self.assertEqual('?', action.nargs)
        self.assertEqual(1, action.default)
        self.assertEqual(int, action.type)
        self.assertEqual(metavar, action.metavar)
        num = 111
        kwargs = vars(self.parser.parse_args([f'{num}']))
        self.assertDictEqual({session_parser.NUM: num}, kwargs)

        name = f'--{FOO}'
        nargs = '+'
        default = 1
        _type = float
        required = True
        dest = 'foo_bar'
        action = self.parser.add_optional_num_argument(name, nargs=nargs, default=default, type=_type,
                                                       required=required, metavar=metavar, dest=dest)
        self.assertIsInstance(action, Action)
        self.assertEqual(nargs, action.nargs)
        self.assertEqual(default, action.default)
        self.assertEqual(_type, action.type)
        self.assertEqual(required, action.required)
        self.assertEqual(metavar, action.metavar)
        self.assertEqual(dest, action.dest)
        kwargs = vars(self.parser.parse_args([f'{num}', name, '1', '1.5']))
        self.assertDictEqual({session_parser.NUM: num, dest: [1.0, 1.5]}, kwargs)
