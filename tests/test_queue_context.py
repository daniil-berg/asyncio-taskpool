"""Unittests for the `asyncio_taskpool.queue_context` module."""

from typing import Any
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from asyncio_taskpool.queue_context import Queue


class QueueTestCase(IsolatedAsyncioTestCase):
    def test_item_processed(self) -> None:
        queue: Queue[Any] = Queue()
        queue._unfinished_tasks = 1000  # type: ignore[attr-defined]
        queue.item_processed()
        self.assertEqual(999, queue._unfinished_tasks)  # type: ignore[attr-defined]

    @patch.object(Queue, "item_processed")
    async def test_contextmanager(self, mock_item_processed: MagicMock) -> None:
        queue: Queue[Any] = Queue()
        item = "foo"
        queue.put_nowait(item)
        async with queue as item_from_queue:
            self.assertEqual(item, item_from_queue)
            mock_item_processed.assert_not_called()
        mock_item_processed.assert_called_once_with()
