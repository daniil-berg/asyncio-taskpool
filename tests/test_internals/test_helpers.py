"""
Unittests for the `asyncio_taskpool.helpers` module.
"""

import importlib
from typing import Any, Tuple, cast
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import MagicMock, AsyncMock, NonCallableMagicMock, call, patch

from asyncio_taskpool.internals import constants
from asyncio_taskpool.internals import helpers


class HelpersTestCase(IsolatedAsyncioTestCase):

    async def test_execute_optional(self) -> None:
        f, args, kwargs = NonCallableMagicMock(), [1, 2], None
        self.assertIsNone(await helpers.execute_optional(f, args, kwargs))

        expected_output = 'foo'
        f = MagicMock(return_value=expected_output)
        output = await helpers.execute_optional(f, args, kwargs)
        self.assertEqual(expected_output, output)
        f.assert_called_once_with(*args)

        f.reset_mock()

        kwargs = {'a': 100, 'b': 200}
        output = await helpers.execute_optional(f, args, kwargs)
        self.assertEqual(expected_output, output)
        f.assert_called_once_with(*args, **kwargs)

        f = AsyncMock(return_value=expected_output)
        output = await helpers.execute_optional(f, args, kwargs)
        self.assertEqual(expected_output, output)
        f.assert_awaited_once_with(*args, **kwargs)

    def test_star_function(self) -> None:
        expected_output = 'bar'
        mock_inner_call = MagicMock(return_value=expected_output)
        def f(arg: Tuple[int, int, int]) -> str:
            return cast(str, mock_inner_call(arg))
        a = (1, 2, 3)
        output = helpers.star_function(f, a, 0)
        self.assertEqual(expected_output, output)
        mock_inner_call.assert_called_once_with(a)

        mock_inner_call.reset_mock()

        def g(x: int, y: int, z: int) -> str:
            return cast(str, mock_inner_call(x, y, z))
        output = helpers.star_function(g, a, 1)
        self.assertEqual(expected_output, output)
        mock_inner_call.assert_called_once_with(*a)

        mock_inner_call.reset_mock()

        def h(x: int, y: int) -> str:
            return cast(str, mock_inner_call(x=x, y=y))
        kw = {'x': 1, 'y': 2}
        output = helpers.star_function(h, kw, 2)
        self.assertEqual(expected_output, output)
        mock_inner_call.assert_called_once_with(**kw)

        with self.assertRaises(ValueError):
            helpers.star_function(f, a, 3)  # type: ignore[call-overload]
        with self.assertRaises(ValueError):
            helpers.star_function(f, a, -1)  # type: ignore[call-overload]
        with self.assertRaises(ValueError):
            helpers.star_function(f, a, 123456789)  # type: ignore[call-overload]

    def test_get_first_doc_line(self) -> None:
        self.assertIsNone(helpers.get_first_doc_line(lambda: "foo"))
        expected_output = 'foo bar baz'
        mock_obj = MagicMock(__doc__=f"""{expected_output} 
        something else
        
        even more
        """)
        output = helpers.get_first_doc_line(mock_obj)
        self.assertEqual(expected_output, output)

    async def test_return_or_exception(self) -> None:
        expected_output = '420'
        mock_func = AsyncMock(return_value=expected_output)
        args = (1, 3, 5)
        kwargs = {'a': 1, 'b': 2, 'c': 'foo'}
        output = await helpers.return_or_exception(mock_func, *args, **kwargs)
        self.assertEqual(expected_output, output)
        mock_func.assert_awaited_once_with(*args, **kwargs)

        mock_func = MagicMock(return_value=expected_output)
        output = await helpers.return_or_exception(mock_func, *args, **kwargs)
        self.assertEqual(expected_output, output)
        mock_func.assert_called_once_with(*args, **kwargs)

        class TestException(Exception):
            pass
        test_exception = TestException()
        mock_func = MagicMock(side_effect=test_exception)
        output = await helpers.return_or_exception(mock_func, *args, **kwargs)
        self.assertEqual(test_exception, output)
        mock_func.assert_called_once_with(*args, **kwargs)

    def test_resolve_dotted_path(self) -> None:
        from logging import WARNING
        from urllib.request import urlopen
        self.assertEqual(WARNING, helpers.resolve_dotted_path('logging.WARNING'))
        self.assertEqual(urlopen, helpers.resolve_dotted_path('urllib.request.urlopen'))
        with patch.object(helpers, 'import_module', return_value=object) as mock_import_module:
            with self.assertRaises(AttributeError):
                helpers.resolve_dotted_path('foo.bar.baz')
            mock_import_module.assert_has_calls([call('foo'), call('foo.bar')])


class ClassMethodWorkaroundTestCase(TestCase):
    def test_init(self) -> None:
        def func(_cls: object) -> str: return 'foo'
        def getter(_cls: object) -> str: return 'bar'
        prop = property(getter)
        instance = helpers.ClassMethodWorkaround(func)
        self.assertIs(func, instance._getter)
        instance = helpers.ClassMethodWorkaround(prop)
        self.assertIs(getter, instance._getter)

    @patch.object(helpers.ClassMethodWorkaround, '__init__', return_value=None)
    def test_get(self, _mock_init: MagicMock) -> None:
        def func(x: MagicMock) -> Any: return x.__name__
        instance = helpers.ClassMethodWorkaround(MagicMock())
        instance._getter = func
        obj, cls = None, MagicMock
        expected_output = 'MagicMock'
        output = instance.__get__(obj, cls)
        self.assertEqual(expected_output, output)

        obj = MagicMock(__name__='bar')
        expected_output = 'bar'
        output = instance.__get__(obj, cls)
        self.assertEqual(expected_output, output)

        output = instance.__get__(obj, None)
        self.assertEqual(expected_output, output)

    def test_correct_class(self) -> None:
        is_older_python = constants.PYTHON_BEFORE_39
        try:
            constants.PYTHON_BEFORE_39 = True
            importlib.reload(helpers)
            self.assertIs(helpers.ClassMethodWorkaround, helpers.classmethod)
            constants.PYTHON_BEFORE_39 = False
            importlib.reload(helpers)
            self.assertIs(classmethod, helpers.classmethod)
        finally:
            constants.PYTHON_BEFORE_39 = is_older_python
