import asyncio
import logging

from asyncio_taskpool import SimpleTaskPool, UnixControlServer
from asyncio_taskpool.constants import PACKAGE_NAME


logging.getLogger().setLevel(logging.NOTSET)
logging.getLogger(PACKAGE_NAME).addHandler(logging.StreamHandler())


async def work(item: int) -> None:
    """The non-blocking sleep simulates something like an I/O operation that can be done asynchronously."""
    await asyncio.sleep(1)
    print("worked on", item)


async def worker(q: asyncio.Queue) -> None:
    """Simulates doing asynchronous work that takes a little bit of time to finish."""
    # We only want the worker to stop, when its task is cancelled; therefore we start an infinite loop.
    while True:
        # We want to block here, until we can get the next item from the queue.
        item = await q.get()
        # Since we want a nice cleanup upon cancellation, we put the "work" to be done in a `try:` block.
        try:
            await work(item)
        except asyncio.CancelledError:
            # If the task gets cancelled before our current "work" item is finished, we put it back into the queue
            # because a worker must assume that some other worker can and will eventually finish the work on that item.
            q.put_nowait(item)
            # This takes us out of the loop. To enable cleanup we must re-raise the exception.
            raise
        finally:
            # Since putting an item into the queue (even if it has just been taken out), increments the internal
            # `._unfinished_tasks` counter in the queue, we must ensure that it is decremented before we end the
            # iteration or leave the loop. Otherwise, the queue's `.join()` will block indefinitely.
            q.task_done()


async def main() -> None:
    # First, we set up a queue of items that our workers can "work" on.
    q = asyncio.Queue()
    # We just put some integers into our queue, since all our workers actually do, is print an item and sleep for a bit.
    for item in range(100):
        q.put_nowait(item)
    pool = SimpleTaskPool(worker, (q,))  # initializes the pool
    await pool.start(3)  # launches three worker tasks
    control_server_task = await UnixControlServer(pool, path='/tmp/py_asyncio_taskpool.sock').serve_forever()
    # We block until `.task_done()` has been called once by our workers for every item placed into the queue.
    await q.join()
    # Since we don't need any "work" done anymore, we can lock our control server by cancelling the task.
    control_server_task.cancel()
    # Since our workers should now be stuck waiting for more items to pick from the queue, but no items are left,
    # we can now safely cancel their tasks.
    pool.stop_all()
    pool.lock()
    # Finally we allow for all tasks to do do their cleanup, if they need to do any, upon being cancelled.
    # We block until they all return or raise an exception, but since we are not interested in any of their exceptions,
    # we just silently collect their exceptions along with their return values.
    await pool.gather(return_exceptions=True)
    await control_server_task


if __name__ == '__main__':
    asyncio.run(main())
