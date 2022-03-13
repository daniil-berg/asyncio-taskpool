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
Unittests for the `asyncio_taskpool.control.parser` module.
"""


from argparse import ArgumentParser, HelpFormatter, ArgumentDefaultsHelpFormatter, RawTextHelpFormatter, SUPPRESS
from inspect import signature
from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from asyncio_taskpool.control import parser
from asyncio_taskpool.exceptions import HelpRequested


FOO, BAR = 'foo', 'bar'


class ControlServerTestCase(TestCase):

    def setUp(self) -> None:
        self.help_formatter_factory_patcher = patch.object(parser.ControlParser, 'help_formatter_factory')
        self.mock_help_formatter_factory = self.help_formatter_factory_patcher.start()
        self.mock_help_formatter_factory.return_value = RawTextHelpFormatter
        self.stream_writer, self.terminal_width = MagicMock(), 420
        self.kwargs = {
            'stream_writer': self.stream_writer,
            'terminal_width': self.terminal_width,
            parser.FORMATTER_CLASS: FOO
        }
        self.parser = parser.ControlParser(**self.kwargs)

    def tearDown(self) -> None:
        self.help_formatter_factory_patcher.stop()

    def test_help_formatter_factory(self):
        self.help_formatter_factory_patcher.stop()

        class MockBaseClass(HelpFormatter):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)

        terminal_width = 123456789
        cls = parser.ControlParser.help_formatter_factory(terminal_width, MockBaseClass)
        self.assertTrue(issubclass(cls, MockBaseClass))
        instance = cls('prog')
        self.assertEqual(terminal_width, getattr(instance, '_width'))

        cls = parser.ControlParser.help_formatter_factory(terminal_width)
        self.assertTrue(issubclass(cls, ArgumentDefaultsHelpFormatter))
        instance = cls('prog')
        self.assertEqual(terminal_width, getattr(instance, '_width'))

    def test_init(self):
        self.assertIsInstance(self.parser, ArgumentParser)
        self.assertEqual(self.stream_writer, self.parser._stream_writer)
        self.assertEqual(self.terminal_width, self.parser._terminal_width)
        self.mock_help_formatter_factory.assert_called_once_with(self.terminal_width, FOO)
        self.assertFalse(getattr(self.parser, 'exit_on_error'))
        self.assertEqual(RawTextHelpFormatter, getattr(self.parser, 'formatter_class'))
        self.assertSetEqual(set(), self.parser._flags)
        self.assertIsNone(self.parser._commands)

    @patch.object(parser, 'get_first_doc_line')
    def test_add_function_command(self, mock_get_first_doc_line: MagicMock):
        def foo_bar(): pass
        mock_subparser = MagicMock()
        mock_add_parser = MagicMock(return_value=mock_subparser)
        self.parser._commands = MagicMock(add_parser=mock_add_parser)
        mock_get_first_doc_line.return_value = mock_help = 'help 123'
        kwargs = {FOO: 1, BAR: 2, parser.DESCRIPTION: FOO + BAR}
        expected_name = 'foo-bar'
        expected_kwargs = {parser.NAME: expected_name, parser.PROG: expected_name, parser.HELP: mock_help} | kwargs
        to_omit = ['abc', 'xyz']
        output = self.parser.add_function_command(foo_bar, omit_params=to_omit, **kwargs)
        self.assertEqual(mock_subparser, output)
        mock_add_parser.assert_called_once_with(**expected_kwargs)
        mock_subparser.add_function_args.assert_called_once_with(foo_bar, to_omit)

    @patch.object(parser, 'get_first_doc_line')
    def test_add_property_command(self, mock_get_first_doc_line: MagicMock):
        def get_prop(_self): pass
        def set_prop(_self, _value): pass
        prop = property(get_prop)
        mock_subparser = MagicMock()
        mock_add_parser = MagicMock(return_value=mock_subparser)
        self.parser._commands = MagicMock(add_parser=mock_add_parser)
        mock_get_first_doc_line.return_value = mock_help = 'help 123'
        kwargs = {FOO: 1, BAR: 2, parser.DESCRIPTION: FOO + BAR}
        expected_name = 'get-prop'
        expected_kwargs = {parser.NAME: expected_name, parser.PROG: expected_name, parser.HELP: mock_help} | kwargs
        output = self.parser.add_property_command(prop, **kwargs)
        self.assertEqual(mock_subparser, output)
        mock_get_first_doc_line.assert_called_once_with(get_prop)
        mock_add_parser.assert_called_once_with(**expected_kwargs)
        mock_subparser.add_function_arg.assert_not_called()

        mock_get_first_doc_line.reset_mock()
        mock_add_parser.reset_mock()

        prop = property(get_prop, set_prop)
        expected_help = f"Get/set the `.{expected_name}` property"
        expected_kwargs = {parser.NAME: expected_name, parser.PROG: expected_name, parser.HELP: expected_help} | kwargs
        output = self.parser.add_property_command(prop, **kwargs)
        self.assertEqual(mock_subparser, output)
        mock_get_first_doc_line.assert_has_calls([call(get_prop), call(set_prop)])
        mock_add_parser.assert_called_once_with(**expected_kwargs)
        mock_subparser.add_function_arg.assert_called_once_with(
            tuple(signature(set_prop).parameters.values())[1],
            nargs='?',
            default=SUPPRESS,
            help=f"If provided: {mock_help} If omitted: {mock_help}"
        )

    @patch.object(parser.ControlParser, 'add_property_command')
    @patch.object(parser.ControlParser, 'add_function_command')
    def test_add_class_commands(self, mock_add_function_command: MagicMock, mock_add_property_command: MagicMock):
        class FooBar:
            some_attribute = None

            def _protected(self, _): pass

            def __private(self, _): pass

            def to_omit(self, _): pass

            def method(self, _): pass

            @property
            def prop(self): return None

        mock_set_defaults = MagicMock()
        mock_subparser = MagicMock(set_defaults=mock_set_defaults)
        mock_add_function_command.return_value = mock_add_property_command.return_value = mock_subparser
        x = 'x'
        common_kwargs = {parser.STREAM_WRITER: self.parser._stream_writer,
                         parser.CLIENT_INFO.TERMINAL_WIDTH: self.parser._terminal_width}
        expected_output = {'method': mock_subparser, 'prop': mock_subparser}
        output = self.parser.add_class_commands(FooBar, public_only=True, omit_members=['to_omit'], member_arg_name=x)
        self.assertDictEqual(expected_output, output)
        mock_add_function_command.assert_called_once_with(FooBar.method, **common_kwargs)
        mock_add_property_command.assert_called_once_with(FooBar.prop, FooBar.__name__, **common_kwargs)
        mock_set_defaults.assert_has_calls([call(**{x: FooBar.method}), call(**{x: FooBar.prop})])

    @patch.object(parser.ArgumentParser, 'add_subparsers')
    def test_add_subparsers(self, mock_base_add_subparsers: MagicMock):
        args, kwargs = [1, 2, 42], {FOO: 123, BAR: 456}
        mock_base_add_subparsers.return_value = mock_action = MagicMock()
        output = self.parser.add_subparsers(*args, **kwargs)
        self.assertEqual(mock_action, output)
        mock_base_add_subparsers.assert_called_once_with(*args, **kwargs)

    def test__print_message(self):
        self.stream_writer.write = MagicMock()
        self.assertIsNone(self.parser._print_message(''))
        self.stream_writer.write.assert_not_called()
        msg = 'foo bar baz'
        self.assertIsNone(self.parser._print_message(msg))
        self.stream_writer.write.assert_called_once_with(msg.encode())

    @patch.object(parser.ControlParser, '_print_message')
    def test_exit(self, mock__print_message: MagicMock):
        self.assertIsNone(self.parser.exit(123, ''))
        mock__print_message.assert_not_called()
        msg = 'foo bar baz'
        self.assertIsNone(self.parser.exit(123, msg))
        mock__print_message.assert_called_once_with(msg)

    @patch.object(parser.ArgumentParser, 'error')
    def test_error(self, mock_supercls_error: MagicMock):
        with self.assertRaises(HelpRequested):
            self.parser.error(FOO + BAR)
        mock_supercls_error.assert_called_once_with(message=FOO + BAR)

    @patch.object(parser.ArgumentParser, 'print_help')
    def test_print_help(self, mock_print_help: MagicMock):
        arg = MagicMock()
        with self.assertRaises(HelpRequested):
            self.parser.print_help(arg)
        mock_print_help.assert_called_once_with(arg)

    @patch.object(parser, 'get_arg_type_wrapper')
    @patch.object(parser.ArgumentParser, 'add_argument')
    def test_add_function_arg(self, mock_add_argument: MagicMock, mock_get_arg_type_wrapper: MagicMock):
        mock_add_argument.return_value = expected_output = 'action'
        mock_get_arg_type_wrapper.return_value = mock_type = 'fake'

        foo_type, args_type, bar_type, baz_type, boo_type = tuple, str, int, float, complex
        bar_default, baz_default, boo_default = 1, 0.1, 1j

        def func(foo: foo_type, *args: args_type, bar: bar_type = bar_default, baz: baz_type = baz_default,
                 boo: boo_type = boo_default, flag: bool = False):
            return foo, args, bar, baz, boo, flag

        param_foo, param_args, param_bar, param_baz, param_boo, param_flag = signature(func).parameters.values()
        kwargs = {FOO + BAR: 'xyz'}
        self.assertEqual(expected_output, self.parser.add_function_arg(param_foo, **kwargs))
        mock_add_argument.assert_called_once_with('foo', type=mock_type, **kwargs)
        mock_get_arg_type_wrapper.assert_called_once_with(foo_type)

        mock_add_argument.reset_mock()
        mock_get_arg_type_wrapper.reset_mock()

        self.assertEqual(expected_output, self.parser.add_function_arg(param_args, **kwargs))
        mock_add_argument.assert_called_once_with('args', nargs='*', type=mock_type, **kwargs)
        mock_get_arg_type_wrapper.assert_called_once_with(args_type)

        mock_add_argument.reset_mock()
        mock_get_arg_type_wrapper.reset_mock()

        self.assertEqual(expected_output, self.parser.add_function_arg(param_bar, **kwargs))
        mock_add_argument.assert_called_once_with('-b', '--bar', default=bar_default, type=mock_type, **kwargs)
        mock_get_arg_type_wrapper.assert_called_once_with(bar_type)

        mock_add_argument.reset_mock()
        mock_get_arg_type_wrapper.reset_mock()

        self.assertEqual(expected_output, self.parser.add_function_arg(param_baz, **kwargs))
        mock_add_argument.assert_called_once_with('-B', '--baz', default=baz_default, type=mock_type, **kwargs)
        mock_get_arg_type_wrapper.assert_called_once_with(baz_type)

        mock_add_argument.reset_mock()
        mock_get_arg_type_wrapper.reset_mock()

        self.assertEqual(expected_output, self.parser.add_function_arg(param_boo, **kwargs))
        mock_add_argument.assert_called_once_with('--boo', default=boo_default, type=mock_type, **kwargs)
        mock_get_arg_type_wrapper.assert_called_once_with(boo_type)

        mock_add_argument.reset_mock()
        mock_get_arg_type_wrapper.reset_mock()

        self.assertEqual(expected_output, self.parser.add_function_arg(param_flag, **kwargs))
        mock_add_argument.assert_called_once_with('-f', '--flag', action='store_true', **kwargs)
        mock_get_arg_type_wrapper.assert_not_called()

    @patch.object(parser.ControlParser, 'add_function_arg')
    def test_add_function_args(self, mock_add_function_arg: MagicMock):
        def func(foo: str, *args: int, bar: float = 0.1):
            return foo, args, bar
        _, param_args, param_bar = signature(func).parameters.values()
        self.assertIsNone(self.parser.add_function_args(func, omit=['foo']))
        mock_add_function_arg.assert_has_calls([
            call(param_args, help=repr(param_args.annotation)),
            call(param_bar, help=repr(param_bar.annotation)),
        ])


class RestTestCase(TestCase):
    def test_get_arg_type_wrapper(self):
        type_wrap = parser.get_arg_type_wrapper(int)
        self.assertEqual(SUPPRESS, type_wrap(SUPPRESS))
        self.assertEqual(13, type_wrap('13'))
