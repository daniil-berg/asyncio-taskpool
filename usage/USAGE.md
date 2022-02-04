# Using `asyncio-taskpool`

## Simple example

The minimum required setup is a "worker" coroutine function that can do something asynchronously, a main coroutine function that sets up the `TaskPool` and starts/stops the tasks as desired, eventually awaiting them all. 

The following demo code enables full log output first for additional clarity. It is complete and should work as is.

### Code
```python
import logging
import asyncio

from asyncio_taskpool.pool import TaskPool


logging.getLogger().setLevel(logging.NOTSET)
logging.getLogger('asyncio_taskpool').addHandler(logging.StreamHandler())


async def work(n: int) -> None:
    """
    Pseudo-worker function. 
    Counts up to an integer with a second of sleep before each iteration.
    In a real-world use case, a worker function should probably have access 
    to some synchronisation primitive or shared resource to distribute work 
    between an arbitrary number of workers.
    """
    for i in range(n):
        await asyncio.sleep(1)
        print("did", i)


async def main() -> None:
    pool = TaskPool(work, (5,))  # initializes the pool; no work is being done yet
    pool.start(3)  # launches work tasks 0, 1, and 2
    await asyncio.sleep(1.5)  # lets the tasks work for a bit
    pool.start()  # launches work task 3
    await asyncio.sleep(1.5)  # lets the tasks work for a bit
    pool.stop(2)  # cancels tasks 3 and 2
    await pool.close()  # awaits all tasks, then flushes the pool


if __name__ == '__main__':
    asyncio.run(main())
```

### Output 
Additional comments indicated with `<--`
```
Started work_pool_task_0
Started work_pool_task_1
Started work_pool_task_2
did 0
did 0
did 0
Started work_pool_task_3
did 1
did 1
did 1
did 0    <-- notice that the newly created task begins counting at 0 
did 2
did 2    <-- two taks were stopped; only tasks 0 and 1 continue "working"
Cancelling work_pool_task_2 ...
Cancelled work_pool_task_2
Exiting work_pool_task_2
Cancelling work_pool_task_3 ...
Cancelled work_pool_task_3
Exiting work_pool_task_3
did 3
did 3
Exiting work_pool_task_0
Exiting work_pool_task_1
did 4
did 4
```

## Advanced example

...
