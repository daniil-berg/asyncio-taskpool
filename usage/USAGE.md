# Using `asyncio-taskpool`

## Minimal example for `SimpleTaskPool`

The minimum required setup is a "worker" coroutine function that can do something asynchronously, a main coroutine function that sets up the `SimpleTaskPool` and starts/stops the tasks as desired, eventually awaiting them all. 

The following demo code enables full log output first for additional clarity. It is complete and should work as is.

### Code
```python
import logging
import asyncio

from asyncio_taskpool.pool import SimpleTaskPool


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
    pool = SimpleTaskPool(work, (5,))  # initializes the pool; no work is being done yet
    pool.start(3)  # launches work tasks 0, 1, and 2
    await asyncio.sleep(1.5)  # lets the tasks work for a bit
    pool.start()  # launches work task 3
    await asyncio.sleep(1.5)  # lets the tasks work for a bit
    pool.stop(2)  # cancels tasks 3 and 2
    pool.close()  # required for the last line
    await pool.gather()  # awaits all tasks, then flushes the pool


if __name__ == '__main__':
    asyncio.run(main())
```

### Output 
```
SimpleTaskPool-0 initialized
Started SimpleTaskPool-0_Task-0
Started SimpleTaskPool-0_Task-1
Started SimpleTaskPool-0_Task-2
did 0
did 0
did 0
Started SimpleTaskPool-0_Task-3
did 1
did 1
did 1
did 0
SimpleTaskPool-0 is closed!
Cancelling SimpleTaskPool-0_Task-3 ...
Cancelled SimpleTaskPool-0_Task-3
Ended SimpleTaskPool-0_Task-3
Cancelling SimpleTaskPool-0_Task-2 ...
Cancelled SimpleTaskPool-0_Task-2
Ended SimpleTaskPool-0_Task-2
did 2
did 2
did 3
did 3
Ended SimpleTaskPool-0_Task-0
Ended SimpleTaskPool-0_Task-1
did 4
did 4
```

## Advanced example

...
