# asyncio-taskpool

**Dynamically manage pools of asyncio tasks**

## Contents
- [Contents](#contents)
- [Summary](#summary)
- [Usage](#usage)
- [Installation](#installation)
- [Dependencies](#dependencies)
- [Testing](#testing)
- [License](#license)

## Summary

A **task pool** is an object with a simple interface for aggregating and dynamically managing asynchronous tasks.

With an interface that is intentionally similar to the [`multiprocessing.Pool`](https://docs.python.org/3/library/multiprocessing.html#module-multiprocessing.pool) class from the standard library, the `TaskPool` provides you such methods as `apply`, `map`, and `starmap` to execute coroutines concurrently as [`asyncio.Task`](https://docs.python.org/3/library/asyncio-task.html#task-object) objects. There is no limitation imposed on what kind of tasks can be run or in what combination, when new ones can be added, or when they can be cancelled.

For a more streamlined use-case, the `SimpleTaskPool` provides an even more intuitive and simple interface at the cost of flexibility.

If you need control over a task pool at runtime, you can launch an asynchronous `ControlServer` to be able to interface with the pool from an outside process or via a network, and stop/start tasks within the pool as you wish.

## Usage

Generally speaking, a task is added to a pool by providing it with a coroutine function reference as well as the arguments for that function. Here is what that could look like in the most simplified form:

```python
from asyncio_taskpool import SimpleTaskPool
...
async def work(_foo, _bar): ...

async def main():
    pool = SimpleTaskPool(work, args=('xyz', 420))
    await pool.start(5)
    ...
    pool.stop(3)
    ...
    await pool.gather_and_close()
```

Since one of the main goals of `asyncio-taskpool` is to be able to start/stop tasks dynamically or "on-the-fly", _most_ of the associated methods are non-blocking _most_ of the time. A notable exception is the `gather_and_close` method for awaiting the return of all tasks in the pool. (It is essentially a glorified wrapper around the [`asyncio.gather`](https://docs.python.org/3/library/asyncio-task.html#asyncio.gather) function.)

For working and fully documented demo scripts see [USAGE.md](usage/USAGE.md).

## Installation

```shell
pip install asyncio-taskpool
```

## Dependencies

Python Version 3.8+, tested on Linux

## Testing

Install `asyncio-taskpool[dev]` dependencies or just manually install [`coverage`](https://coverage.readthedocs.io/en/latest/) with `pip`. 
Execute the [`./coverage.sh`](coverage.sh) shell script to run all unit tests and receive the coverage report.

## License

`asyncio-taskpool` is licensed under the **GNU LGPL version 3.0** specifically.

The full license texts for the [GNU GPLv3.0](COPYING) and the [GNU LGPLv3.0](COPYING.LESSER) are included in this repository. If not, see https://www.gnu.org/licenses/.

---

© 2022 Daniil Fajnberg
