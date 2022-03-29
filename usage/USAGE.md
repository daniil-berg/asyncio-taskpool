# Using `asyncio-taskpool`

## Contents
- [Contents](#contents)
- [Minimal example for `SimpleTaskPool`](#minimal-example-for-simpletaskpool)
- [Advanced example for `TaskPool`](#advanced-example-for-taskpool)
- [Control server example](#control-server-example)

## Minimal example for `SimpleTaskPool`

With a `SimpleTaskPool` the function to execute as well as the arguments with which to execute it must be defined during its initialization (and they cannot be changed later). The only control you have after initialization is how many of such tasks are being run.

The minimum required setup is a "worker" coroutine function that can do something asynchronously, and a main coroutine function that sets up the `SimpleTaskPool`, starts/stops the tasks as desired, and eventually awaits them all. 

The following demo script enables full log output first for additional clarity. It is complete and should work as is.

```python
import logging
import asyncio

from asyncio_taskpool import SimpleTaskPool

logging.getLogger().setLevel(logging.NOTSET)
logging.getLogger('asyncio_taskpool').addHandler(logging.StreamHandler())


async def work(n: int) -> None:
    """
    Pseudo-worker function. 
    Counts up to an integer with a second of sleep before each iteration.
    In a real-world use case, a worker function should probably have access 
    to some synchronisation primitive (such as a queue) or shared resource
    to distribute work between an arbitrary number of workers.
    """
    for i in range(n):
        await asyncio.sleep(1)
        print("> did", i)


async def main() -> None:
    pool = SimpleTaskPool(work, args=(5,))  # initializes the pool; no work is being done yet
    await pool.start(3)  # launches work tasks 0, 1, and 2
    await asyncio.sleep(1.5)  # lets the tasks work for a bit
    await pool.start(1)  # launches work task 3
    await asyncio.sleep(1.5)  # lets the tasks work for a bit
    pool.stop(2)  # cancels tasks 3 and 2 (LIFO order)
    pool.lock()  # required for the last line
    await pool.gather_and_close()  # awaits all tasks, then flushes the pool


if __name__ == '__main__':
    asyncio.run(main())
```

<details>
<summary>Output: (Click to expand)</summary>

```
SimpleTaskPool-0 initialized
Started SimpleTaskPool-0_Task-0
Started SimpleTaskPool-0_Task-1
Started SimpleTaskPool-0_Task-2
> did 0
> did 0
> did 0
Started SimpleTaskPool-0_Task-3
> did 1
> did 1
> did 1
> did 0
> did 2
> did 2
SimpleTaskPool-0 is locked!
Cancelling SimpleTaskPool-0_Task-2 ...
Cancelled SimpleTaskPool-0_Task-2
Ended SimpleTaskPool-0_Task-2
Cancelling SimpleTaskPool-0_Task-3 ...
Cancelled SimpleTaskPool-0_Task-3
Ended SimpleTaskPool-0_Task-3
> did 3
> did 3
Ended SimpleTaskPool-0_Task-0
Ended SimpleTaskPool-0_Task-1
> did 4
> did 4
```
</details>

## Advanced example for `TaskPool`

This time, we want to start tasks from _different_ coroutine functions **and** with _different_ arguments. For this we need an instance of the more generalized `TaskPool` class.

As with the simple example, we need "worker" coroutine functions that can do something asynchronously, as well as a main coroutine function that sets up the pool, starts the tasks, and eventually awaits them.

The following demo script enables full log output first for additional clarity. It is complete and should work as is.

```python
import logging
import asyncio

from asyncio_taskpool import TaskPool

logging.getLogger().setLevel(logging.NOTSET)
logging.getLogger('asyncio_taskpool').addHandler(logging.StreamHandler())


async def work(start: int, stop: int, step: int = 1) -> None:
    """Pseudo-worker function counting through a range with a second of sleep in between each iteration."""
    for i in range(start, stop, step):
        await asyncio.sleep(1)
        print("> work with", i)


async def other_work(a: int, b: int) -> None:
    """Different pseudo-worker counting through a range with half a second of sleep in between each iteration."""
    for i in range(a, b):
        await asyncio.sleep(0.5)
        print("> other_work with", i)


async def main() -> None:
    # Initialize a new task pool instance and limit its size to 3 tasks.
    pool = TaskPool(3)
    # Queue up two tasks (IDs 0 and 1) to run concurrently (with the same keyword-arguments).
    print("> Called `apply`")
    await pool.apply(work, kwargs={'start': 100, 'stop': 200, 'step': 10}, num=2)
    # Let the tasks work for a bit.
    await asyncio.sleep(1.5)
    # Now, let us enqueue four more tasks (which will receive IDs 2, 3, 4, and 5), each created with different 
    # positional arguments by using `starmap`, but we want no more than two of those to run concurrently.
    # Since we set our pool size to 3, and already have two tasks working within the pool,
    # only the first one of these will start immediately (and receive ID 2).
    # The second one will start (with ID 3), only once there is room in the pool,
    # which -- in this example -- will be the case after ID 2 ends.
    # Once there is room in the pool again, the third and fourth will each start (with IDs 4 and 5)
    # only once there is room in the pool and no more than one other task of these new ones is running.
    args_list = [(0, 10), (10, 20), (20, 30), (30, 40)]
    await pool.starmap(other_work, args_list, num_concurrent=2)
    print("> Called `starmap`")
    # Now we lock the pool, so that we can safely await all our tasks.
    pool.lock()
    # Finally, we block, until all tasks have ended.
    print("> Calling `gather_and_close`...")
    await pool.gather_and_close()
    print("> Done.")


if __name__ == '__main__':
    asyncio.run(main())
```

<details>
<summary>Output: (Click to expand)</summary>

```
TaskPool-0 initialized
Started TaskPool-0_Task-0
Started TaskPool-0_Task-1
> Called `apply`
> work with 100
> work with 100
> Called `starmap`   <--- notice that this immediately returns, even before Task-2 is started
> Calling `gather_and_close`...    <--- this blocks `main()` until all tasks have ended
TaskPool-0 is locked!
Started TaskPool-0_Task-2    <--- at this point the pool is full
> work with 110
> work with 110
> other_work with 0
> other_work with 1
> work with 120
> work with 120
> other_work with 2
> other_work with 3
> work with 130
> work with 130
> other_work with 4
> other_work with 5
> work with 140
> work with 140
> other_work with 6
> other_work with 7
> work with 150
> work with 150
> other_work with 8
Ended TaskPool-0_Task-2    <--- this frees up room for one more task from `starmap`
Started TaskPool-0_Task-3
> other_work with 9
> work with 160
> work with 160
> other_work with 10
> other_work with 11
> work with 170
> work with 170
> other_work with 12
> other_work with 13
> work with 180
> work with 180
> other_work with 14
> other_work with 15
Ended TaskPool-0_Task-0
Ended TaskPool-0_Task-1    <--- these two end and free up two more slots in the pool
Started TaskPool-0_Task-4    <--- since `num_concurrent` is set to 2, Task-5 will not start
> work with 190
> work with 190
> other_work with 16
> other_work with 17
> other_work with 20
> other_work with 18
> other_work with 21
Ended TaskPool-0_Task-3    <--- now that only Task-4 of the group remains, Task-5 starts
Started TaskPool-0_Task-5
> other_work with 19
> other_work with 22
> other_work with 23
> other_work with 30
> other_work with 24
> other_work with 31
> other_work with 25
> other_work with 32
> other_work with 26
> other_work with 33
> other_work with 27
> other_work with 34
> other_work with 28
> other_work with 35
> other_work with 29
> other_work with 36
Ended TaskPool-0_Task-4
> other_work with 37
> other_work with 38
> other_work with 39
Ended TaskPool-0_Task-5
> Done.
```

(Added comments with `<---` next to the output lines.)

Keep in mind that the logger and `print` asynchronously write to `stdout`, so the order of lines in your output may be slightly different.
</details>

## Control server example

One of the main features of `asyncio-taskpool` is the ability to control a task pool "from the outside" at runtime.

The [example_server.py](./example_server.py) script launches a couple of worker tasks within a `SimpleTaskPool` instance and then starts a `TCPControlServer` instance for that task pool. The server is configured to locally bind to port `9999` and is stopped automatically after the "work" is done.

To run the script:
```shell
python usage/example_server.py
```

You can then connect to the server via the command line interface:
```shell
python -m asyncio_taskpool.control tcp localhost 9999
```

The CLI starts a `TCPControlClient` that connects to our example server. Once the connection is established, it gives you an input prompt allowing you to issue commands to the task pool:
```
Connected to SimpleTaskPool-0
Type '-h' to get help and usage instructions for all available commands.

>
```

It may be useful to run the server script and the client interface in two separate terminal windows side by side. The server script is configured with a verbose logger and will react to any commands issued by the client with detailed log messages in the terminal.

---

© 2022 Daniil Fajnberg
