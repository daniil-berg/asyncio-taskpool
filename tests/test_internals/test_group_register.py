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
Unittests for the `asyncio_taskpool.group_register` module.
"""


from asyncio.locks import Lock
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from asyncio_taskpool.internals import group_register

FOO, BAR = 'foo', 'bar'


class TaskGroupRegisterTestCase(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.reg = group_register.TaskGroupRegister()

    def test_init(self):
        ids = [FOO, BAR, 1, 2]
        reg = group_register.TaskGroupRegister(*ids)
        self.assertSetEqual(set(ids), reg._ids)
        self.assertIsInstance(reg._lock, Lock)

    def test___contains__(self):
        self.reg._ids = {1, 2, 3}
        for i in self.reg._ids:
            self.assertTrue(i in self.reg)
        self.assertFalse(4 in self.reg)

    @patch.object(group_register, 'iter', return_value=FOO)
    def test___iter__(self, mock_iter: MagicMock):
        self.assertEqual(FOO, self.reg.__iter__())
        mock_iter.assert_called_once_with(self.reg._ids)

    def test___len__(self):
        self.reg._ids = [1, 2, 3, 4]
        self.assertEqual(4, len(self.reg))

    def test_add(self):
        self.assertSetEqual(set(), self.reg._ids)
        self.assertIsNone(self.reg.add(123))
        self.assertSetEqual({123}, self.reg._ids)

    def test_discard(self):
        self.reg._ids = {123}
        self.assertIsNone(self.reg.discard(0))
        self.assertIsNone(self.reg.discard(999))
        self.assertIsNone(self.reg.discard(123))
        self.assertSetEqual(set(), self.reg._ids)

    async def test_acquire(self):
        self.assertFalse(self.reg._lock.locked())
        await self.reg.acquire()
        self.assertTrue(self.reg._lock.locked())

    def test_release(self):
        self.reg._lock._locked = True
        self.assertTrue(self.reg._lock.locked())
        self.reg.release()
        self.assertFalse(self.reg._lock.locked())

    async def test_contextmanager(self):
        self.assertFalse(self.reg._lock.locked())
        async with self.reg as nothing:
            self.assertIsNone(nothing)
            self.assertTrue(self.reg._lock.locked())
        self.assertFalse(self.reg._lock.locked())
