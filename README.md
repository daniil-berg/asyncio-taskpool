# asyncio-taskpool

Dynamically manage pools of asyncio tasks

## Usage

Demo:
```python
import logging
import asyncio

from asyncio_taskpool.pool import TaskPool


logging.getLogger().setLevel(logging.NOTSET)
logging.getLogger('asyncio_taskpool').addHandler(logging.StreamHandler())


async def work(n):
    for i in range(n):
        await asyncio.sleep(1)
        print("did", i)


async def main():
    pool = TaskPool(work, (5,))  # initializes the pool
    pool.start(3)  # launches work tasks 0, 1, and 2
    await asyncio.sleep(1.5)
    pool.start()  # launches work task 3
    await asyncio.sleep(1.5)
    pool.stop(2)  # cancels tasks 3 and 2
    await pool.gather()  # awaits all tasks, then flushes the pool


if __name__ == '__main__':
    asyncio.run(main())
```

Output:
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
did 0
did 2
did 2
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

## Installation

`pip install asyncio-taskpool`

## Dependencies

Python Version 3.8+, tested on Linux

## Building from source

Run `python -m build`
