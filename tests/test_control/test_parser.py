"""Unittests for the `asyncio_taskpool.control.parser` module."""

from argparse import (
    SUPPRESS,
    ArgumentDefaultsHelpFormatter,
    ArgumentParser,
    ArgumentTypeError,
    HelpFormatter,
    RawTextHelpFormatter,
)
from ast import literal_eval
from inspect import signature
from typing import Any, Iterable, Tuple, Type, Union
from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from asyncio_taskpool.control import parser
from asyncio_taskpool.exceptions import (
    HelpRequested,
    ParserError,
    SubParsersNotInitialized,
)
from asyncio_taskpool.internals.constants import CLIENT_INFO
from asyncio_taskpool.internals.helpers import resolve_dotted_path
from asyncio_taskpool.internals.types import (
    AnyCoroutineFunc,
    ArgsT,
    CancelCB,
    EndCB,
    KwArgsT,
)

FOO, BAR = "foo", "bar"


class ControlParserTestCase(TestCase):
    def setUp(self) -> None:
        self.help_formatter_factory_patcher = patch.object(
            parser.ControlParser, "help_formatter_factory"
        )
        self.mock_help_formatter_factory = (
            self.help_formatter_factory_patcher.start()
        )
        self.mock_help_formatter_factory.return_value = RawTextHelpFormatter
        self.stream, self.terminal_width = MagicMock(), 420
        self.parser = parser.ControlParser(
            stream=self.stream,
            terminal_width=self.terminal_width,
            formatter_class=FOO,
        )

    def tearDown(self) -> None:
        self.help_formatter_factory_patcher.stop()

    def test_help_formatter_factory(self) -> None:
        self.help_formatter_factory_patcher.stop()

        class MockBaseClass(HelpFormatter):
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                super().__init__(*args, **kwargs)

        terminal_width = 123456789
        cls: Union[Type[MockBaseClass], Type[ArgumentDefaultsHelpFormatter]]
        instance: Union[MockBaseClass, ArgumentDefaultsHelpFormatter]
        cls = parser.ControlParser.help_formatter_factory(
            terminal_width, MockBaseClass
        )
        self.assertTrue(issubclass(cls, MockBaseClass))
        instance = cls("prog")
        self.assertEqual(terminal_width, instance._width)

        cls = parser.ControlParser.help_formatter_factory(terminal_width)
        self.assertTrue(issubclass(cls, ArgumentDefaultsHelpFormatter))
        instance = cls("prog")
        self.assertEqual(terminal_width, instance._width)

    def test_init(self) -> None:
        self.assertIsInstance(self.parser, ArgumentParser)
        self.assertEqual(self.stream, self.parser._stream)
        self.assertEqual(self.terminal_width, self.parser._terminal_width)
        self.mock_help_formatter_factory.assert_called_once_with(
            self.terminal_width, FOO
        )
        self.assertEqual(RawTextHelpFormatter, self.parser.formatter_class)
        self.assertSetEqual(set(), self.parser._flags)
        self.assertIsNone(self.parser._commands)

    @patch.object(parser, "get_first_doc_line")
    def test_add_function_command(
        self, mock_get_first_doc_line: MagicMock
    ) -> None:
        def foo_bar() -> None:
            pass

        with self.assertRaises(SubParsersNotInitialized):
            self.parser.add_function_command(foo_bar)
        mock_subparser = MagicMock()
        mock_add_parser = MagicMock(return_value=mock_subparser)
        self.parser._commands = MagicMock(add_parser=mock_add_parser)
        mock_get_first_doc_line.return_value = mock_help = "help 123"
        kwargs = {FOO: 1, BAR: 2, parser.DESCRIPTION: FOO + BAR}
        expected_name = "foo-bar"
        expected_kwargs = {
            parser.NAME: expected_name,
            parser.PROG: expected_name,
            parser.HELP: mock_help,
            **kwargs,
        }
        to_omit = ["abc", "xyz"]
        output = self.parser.add_function_command(
            foo_bar, omit_params=to_omit, **kwargs  # type: ignore[arg-type]
        )
        self.assertEqual(mock_subparser, output)
        mock_add_parser.assert_called_once_with(**expected_kwargs)
        mock_subparser.add_function_args.assert_called_once_with(
            foo_bar, to_omit
        )

    @patch.object(parser, "get_first_doc_line")
    def test_add_property_command(
        self, mock_get_first_doc_line: MagicMock
    ) -> None:
        def get_prop(_self: Any) -> None:
            pass

        def set_prop(_self: Any, _value: Any) -> None:
            pass

        with self.assertRaises(TypeError):
            self.parser.add_property_command(property(fset=set_prop))
        prop = property(get_prop)
        with self.assertRaises(SubParsersNotInitialized):
            self.parser.add_property_command(prop)
        mock_subparser = MagicMock()
        mock_add_parser = MagicMock(return_value=mock_subparser)
        self.parser._commands = MagicMock(add_parser=mock_add_parser)
        mock_get_first_doc_line.return_value = mock_help = "help 123"
        kwargs = {FOO: 1, BAR: 2, parser.DESCRIPTION: FOO + BAR}
        expected_name = "get-prop"
        expected_kwargs = {
            parser.NAME: expected_name,
            parser.PROG: expected_name,
            parser.HELP: mock_help,
            **kwargs,
        }
        output = self.parser.add_property_command(prop, "", **kwargs)  # type: ignore[arg-type]
        self.assertEqual(mock_subparser, output)
        mock_get_first_doc_line.assert_called_once_with(get_prop)
        mock_add_parser.assert_called_once_with(**expected_kwargs)
        mock_subparser.add_function_arg.assert_not_called()

        mock_get_first_doc_line.reset_mock()
        mock_add_parser.reset_mock()

        prop = property(get_prop, set_prop)
        expected_help = f"Get/set the `.{expected_name}` property"
        expected_kwargs = {
            parser.NAME: expected_name,
            parser.PROG: expected_name,
            parser.HELP: expected_help,
            **kwargs,
        }
        output = self.parser.add_property_command(prop, "", **kwargs)  # type: ignore[arg-type]
        self.assertEqual(mock_subparser, output)
        mock_get_first_doc_line.assert_has_calls(
            [call(get_prop), call(set_prop)]
        )
        mock_add_parser.assert_called_once_with(**expected_kwargs)
        mock_subparser.add_function_arg.assert_called_once_with(
            tuple(signature(set_prop).parameters.values())[1],
            nargs="?",
            default=SUPPRESS,
            help=f"If provided: {mock_help} If omitted: {mock_help}",
        )

    @patch.object(parser.ControlParser, "add_property_command")
    @patch.object(parser.ControlParser, "add_function_command")
    def test_add_class_commands(
        self,
        mock_add_function_command: MagicMock,
        mock_add_property_command: MagicMock,
    ) -> None:
        class FooBar:
            some_attribute = None

            def _protected(self, _: Any) -> None:
                pass

            def __private(self, _: Any) -> None:
                pass

            def to_omit(self, _: Any) -> None:
                pass

            def method(self, _: Any) -> None:
                pass

            @property
            def prop(self) -> None:
                return None

        mock_set_defaults = MagicMock()
        mock_subparser = MagicMock(set_defaults=mock_set_defaults)
        mock_add_function_command.return_value = (
            mock_add_property_command.return_value
        ) = mock_subparser
        x = "x"
        common_kwargs = {
            "stream": self.parser._stream,
            CLIENT_INFO.TERMINAL_WIDTH: self.parser._terminal_width,
        }
        expected_output = {"method": mock_subparser, "prop": mock_subparser}
        output = self.parser.add_class_commands(
            FooBar,
            public_only=True,
            omit_members=["to_omit"],
            member_arg_name=x,
        )
        self.assertDictEqual(expected_output, output)
        mock_add_function_command.assert_called_once_with(
            FooBar.method, **common_kwargs
        )
        mock_add_property_command.assert_called_once_with(
            FooBar.prop, FooBar.__name__, **common_kwargs
        )
        mock_set_defaults.assert_has_calls(
            [call(**{x: FooBar.method}), call(**{x: FooBar.prop})]
        )

    @patch.object(ArgumentParser, "add_subparsers")
    def test_add_subparsers(self, mock_base_add_subparsers: MagicMock) -> None:
        args, kwargs = [1, 2, 42], {FOO: 123, BAR: 456}
        mock_base_add_subparsers.return_value = mock_action = MagicMock()
        output = self.parser.add_subparsers(*args, **kwargs)
        self.assertEqual(mock_action, output)
        mock_base_add_subparsers.assert_called_once_with(*args, **kwargs)

    def test__print_message(self) -> None:
        self.stream.write = MagicMock()
        self.parser._print_message("")
        self.stream.write.assert_not_called()
        msg = "foo bar baz"
        self.parser._print_message(msg)
        self.stream.write.assert_called_once_with(msg)

    @patch.object(parser.ControlParser, "_print_message")
    def test_exit(self, mock__print_message: MagicMock) -> None:
        self.parser.exit(123, "")
        mock__print_message.assert_not_called()
        msg = "foo bar baz"
        self.parser.exit(123, msg)
        mock__print_message.assert_called_once_with(msg)

    @patch.object(ArgumentParser, "error")
    def test_error(self, mock_supercls_error: MagicMock) -> None:
        with self.assertRaises(ParserError):
            self.parser.error(FOO + BAR)
        mock_supercls_error.assert_called_once_with(message=FOO + BAR)

    @patch.object(ArgumentParser, "print_help")
    def test_print_help(self, mock_print_help: MagicMock) -> None:
        arg = MagicMock()
        with self.assertRaises(HelpRequested):
            self.parser.print_help(arg)
        mock_print_help.assert_called_once_with(arg)

    @patch.object(parser, "_get_type_from_annotation")
    @patch.object(ArgumentParser, "add_argument")
    def test_add_function_arg(
        self,
        mock_add_argument: MagicMock,
        mock__get_type_from_annotation: MagicMock,
    ) -> None:
        mock_add_argument.return_value = expected_output = "action"
        mock__get_type_from_annotation.return_value = mock_type = "fake"

        bar_default, baz_default, boo_default = 1, 0.1, 1j

        def func(
            foo: Tuple[Any, ...],
            *args: str,
            bar: int = bar_default,
            baz: float = baz_default,
            boo: complex = boo_default,
            flag: bool = False,
        ) -> Any:
            return foo, args, bar, baz, boo, flag

        (
            param_foo,
            param_args,
            param_bar,
            param_baz,
            param_boo,
            param_flag,
        ) = signature(func).parameters.values()
        kwargs = {FOO + BAR: "xyz"}
        self.assertEqual(
            expected_output, self.parser.add_function_arg(param_foo, **kwargs)
        )
        mock_add_argument.assert_called_once_with(
            "foo", type=mock_type, **kwargs
        )
        mock__get_type_from_annotation.assert_called_once_with(Tuple[Any, ...])

        mock_add_argument.reset_mock()
        mock__get_type_from_annotation.reset_mock()

        self.assertEqual(
            expected_output, self.parser.add_function_arg(param_args, **kwargs)
        )
        mock_add_argument.assert_called_once_with(
            "args", nargs="*", type=mock_type, **kwargs
        )
        mock__get_type_from_annotation.assert_called_once_with(str)

        mock_add_argument.reset_mock()
        mock__get_type_from_annotation.reset_mock()

        self.assertEqual(
            expected_output, self.parser.add_function_arg(param_bar, **kwargs)
        )
        mock_add_argument.assert_called_once_with(
            "-b", "--bar", default=bar_default, type=mock_type, **kwargs
        )
        mock__get_type_from_annotation.assert_called_once_with(int)

        mock_add_argument.reset_mock()
        mock__get_type_from_annotation.reset_mock()

        self.assertEqual(
            expected_output, self.parser.add_function_arg(param_baz, **kwargs)
        )
        mock_add_argument.assert_called_once_with(
            "-B", "--baz", default=baz_default, type=mock_type, **kwargs
        )
        mock__get_type_from_annotation.assert_called_once_with(float)

        mock_add_argument.reset_mock()
        mock__get_type_from_annotation.reset_mock()

        self.assertEqual(
            expected_output, self.parser.add_function_arg(param_boo, **kwargs)
        )
        mock_add_argument.assert_called_once_with(
            "--boo", default=boo_default, type=mock_type, **kwargs
        )
        mock__get_type_from_annotation.assert_called_once_with(complex)

        mock_add_argument.reset_mock()
        mock__get_type_from_annotation.reset_mock()

        self.assertEqual(
            expected_output, self.parser.add_function_arg(param_flag, **kwargs)
        )
        mock_add_argument.assert_called_once_with(
            "-f", "--flag", action="store_true", **kwargs
        )
        mock__get_type_from_annotation.assert_not_called()

    @patch.object(parser.ControlParser, "add_function_arg")
    def test_add_function_args(self, mock_add_function_arg: MagicMock) -> None:
        def func(foo: str, *args: int, bar: float = 0.1) -> Any:
            return foo, args, bar

        _, param_args, param_bar = signature(func).parameters.values()
        self.parser.add_function_args(func, omit=["foo"])
        mock_add_function_arg.assert_has_calls(
            [
                call(param_args, help=repr(param_args.annotation)),
                call(param_bar, help=repr(param_bar.annotation)),
            ]
        )


class RestTestCase(TestCase):
    log_lvl: int

    @classmethod
    def setUpClass(cls) -> None:
        cls.log_lvl = parser.log.level
        parser.log.setLevel(999)

    @classmethod
    def tearDownClass(cls) -> None:
        parser.log.setLevel(cls.log_lvl)

    def test__get_arg_type_wrapper(self) -> None:
        type_wrap = parser._get_arg_type_wrapper(int)
        self.assertEqual("int", type_wrap.__name__)
        self.assertEqual(SUPPRESS, type_wrap(SUPPRESS))
        self.assertEqual(13, type_wrap("13"))

        name = "abcdef"
        mock_type = MagicMock(
            side_effect=[ArgumentTypeError, TypeError, ValueError, Exception],
            __name__=name,
        )
        type_wrap = parser._get_arg_type_wrapper(mock_type)
        self.assertEqual(name, type_wrap.__name__)
        with self.assertRaises(ArgumentTypeError):
            type_wrap(FOO)
        with self.assertRaises(TypeError):
            type_wrap(FOO)
        with self.assertRaises(ValueError):
            type_wrap(FOO)
        with self.assertRaises(ArgumentTypeError):
            type_wrap(FOO)

    @patch.object(parser, "_get_arg_type_wrapper")
    def test__get_type_from_annotation(
        self,
        mock__get_arg_type_wrapper: MagicMock,
    ) -> None:
        mock__get_arg_type_wrapper.return_value = expected_output = FOO + BAR
        dotted_path_ann = [AnyCoroutineFunc, EndCB, CancelCB]
        literal_eval_ann = [ArgsT, KwArgsT, Iterable[ArgsT], Iterable[KwArgsT]]
        any_other_ann = MagicMock()
        for a1 in dotted_path_ann:
            self.assertEqual(
                expected_output, parser._get_type_from_annotation(a1)
            )
        mock__get_arg_type_wrapper.assert_has_calls(
            len(dotted_path_ann) * [call(resolve_dotted_path)]
        )
        mock__get_arg_type_wrapper.reset_mock()
        for a2 in literal_eval_ann:
            self.assertEqual(
                expected_output, parser._get_type_from_annotation(a2)
            )
        mock__get_arg_type_wrapper.assert_has_calls(
            len(literal_eval_ann) * [call(literal_eval)]
        )
        mock__get_arg_type_wrapper.reset_mock()
        self.assertEqual(
            expected_output, parser._get_type_from_annotation(any_other_ann)
        )
        mock__get_arg_type_wrapper.assert_called_once_with(any_other_ann)
