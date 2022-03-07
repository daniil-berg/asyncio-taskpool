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
Unittests for the `asyncio_taskpool.queue_context` module.
"""


from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from asyncio_taskpool.queue_context import Queue


class QueueTestCase(IsolatedAsyncioTestCase):
    def test_item_processed(self):
        queue = Queue()
        queue._unfinished_tasks = 1000
        queue.item_processed()
        self.assertEqual(999, queue._unfinished_tasks)

    @patch.object(Queue, 'item_processed')
    async def test_contextmanager(self, mock_item_processed: MagicMock):
        queue = Queue()
        item = 'foo'
        queue.put_nowait(item)
        async with queue as item_from_queue:
            self.assertEqual(item, item_from_queue)
            mock_item_processed.assert_not_called()
        mock_item_processed.assert_called_once_with()
