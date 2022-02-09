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


from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, AsyncMock, NonCallableMagicMock

from asyncio_taskpool import helpers


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

    async def test_join_queue(self):
        mock_join = AsyncMock()
        mock_queue = MagicMock(join=mock_join)
        self.assertIsNone(await helpers.join_queue(mock_queue))
        mock_join.assert_awaited_once_with()
