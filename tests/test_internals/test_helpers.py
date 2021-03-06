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
Unittests for the `asyncio_taskpool.helpers` module.
"""

import importlib
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import MagicMock, AsyncMock, NonCallableMagicMock, call, patch

from asyncio_taskpool.internals import constants
from asyncio_taskpool.internals import helpers


class HelpersTestCase(IsolatedAsyncioTestCase):

    async def test_execute_optional(self):
        f, args, kwargs = NonCallableMagicMock(), [1, 2], None
        a = [f, args, kwargs]  # to avoid IDE nagging
        self.assertIsNone(await helpers.execute_optional(*a))

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

    def test_star_function(self):
        expected_output = 'bar'
        f = MagicMock(return_value=expected_output)
        a = (1, 2, 3)
        stars = 0
        output = helpers.star_function(f, a, stars)
        self.assertEqual(expected_output, output)
        f.assert_called_once_with(a)

        f.reset_mock()

        stars = 1
        output = helpers.star_function(f, a, stars)
        self.assertEqual(expected_output, output)
        f.assert_called_once_with(*a)

        f.reset_mock()

        a = {'a': 1, 'b': 2}
        stars = 2
        output = helpers.star_function(f, a, stars)
        self.assertEqual(expected_output, output)
        f.assert_called_once_with(**a)

        with self.assertRaises(ValueError):
            helpers.star_function(f, a, 3)
        with self.assertRaises(ValueError):
            helpers.star_function(f, a, -1)
        with self.assertRaises(ValueError):
            helpers.star_function(f, a, 123456789)

    def test_get_first_doc_line(self):
        expected_output = 'foo bar baz'
        mock_obj = MagicMock(__doc__=f"""{expected_output} 
        something else
        
        even more
        """)
        output = helpers.get_first_doc_line(mock_obj)
        self.assertEqual(expected_output, output)

    async def test_return_or_exception(self):
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

    def test_resolve_dotted_path(self):
        from logging import WARNING
        from urllib.request import urlopen
        self.assertEqual(WARNING, helpers.resolve_dotted_path('logging.WARNING'))
        self.assertEqual(urlopen, helpers.resolve_dotted_path('urllib.request.urlopen'))
        with patch.object(helpers, 'import_module', return_value=object) as mock_import_module:
            with self.assertRaises(AttributeError):
                helpers.resolve_dotted_path('foo.bar.baz')
            mock_import_module.assert_has_calls([call('foo'), call('foo.bar')])


class ClassMethodWorkaroundTestCase(TestCase):
    def test_init(self):
        def func(): return 'foo'
        def getter(): return 'bar'
        prop = property(getter)
        instance = helpers.ClassMethodWorkaround(func)
        self.assertIs(func, instance._getter)
        instance = helpers.ClassMethodWorkaround(prop)
        self.assertIs(getter, instance._getter)

    @patch.object(helpers.ClassMethodWorkaround, '__init__', return_value=None)
    def test_get(self, _mock_init: MagicMock):
        def func(x: MagicMock): return x.__name__
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

        cls = None
        output = instance.__get__(obj, cls)
        self.assertEqual(expected_output, output)

    def test_correct_class(self):
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
