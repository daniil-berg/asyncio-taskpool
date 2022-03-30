.. This file is part of asyncio-taskpool.

.. asyncio-taskpool is free software: you can redistribute it and/or modify it under the terms of
   version 3.0 of the GNU Lesser General Public License as published by the Free Software Foundation.

.. asyncio-taskpool is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
   without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
   See the GNU Lesser General Public License for more details.

.. You should have received a copy of the GNU Lesser General Public License along with asyncio-taskpool.
   If not, see <https://www.gnu.org/licenses/>.

.. Copyright © 2022 Daniil Fajnberg


Task pools
==========

What is a task pool?
--------------------

A task pool is an object with a simple interface for aggregating and dynamically managing asynchronous tasks.

To make use of task pools, your code obviously needs to contain coroutine functions (introduced with the :code:`async def` keywords). By adding such functions along with their arguments to a task pool, they are turned into tasks and executed asynchronously.

If you are familiar with the :code:`Pool` class of the `multiprocessing module <https://docs.python.org/3/library/multiprocessing.html#module-multiprocessing.pool>`_ from the standard library, then you should feel at home with the :py:class:`TaskPool <asyncio_taskpool.pool.TaskPool>` class. Obviously, there are major conceptual and functional differences between the two, but the methods provided by the :py:class:`TaskPool <asyncio_taskpool.pool.TaskPool>` follow a very similar logic. If you never worked with process or thread pools, don't worry. Task pools are much simpler.

The :code:`TaskPool` class
--------------------------

There are essentially two distinct use cases for a concurrency pool. You want to

#. execute a function *n* times with the same arguments concurrently or
#. execute a function *n* times with different arguments concurrently.

The first is accomplished with the :py:meth:`TaskPool.apply() <asyncio_taskpool.pool.TaskPool.apply>` method, while the second is accomplished with the :py:meth:`TaskPool.map() <asyncio_taskpool.pool.TaskPool.map>` method and its variations :py:meth:`.starmap() <asyncio_taskpool.pool.TaskPool.starmap>` and :py:meth:`.doublestarmap() <asyncio_taskpool.pool.TaskPool.doublestarmap>`.

Let's take a look at an example. Say you have a coroutine function that takes two queues as arguments: The first one being an input-queue (containing items to work on) and the second one being the output queue (for passing on the results to some other function). Your function may look something like this:

.. code-block:: python
   :caption: work.py
   :name: queue-worker-function

   from asyncio.queues import Queue

   async def queue_worker_function(in_queue: Queue, out_queue: Queue) -> None:
       while True:
           item = await in_queue.get()
           ... # Do some work on the item and arrive at a result.
           await out_queue.put(result)

How would we go about concurrently executing this function, say 5 times? There are (as always) a number of ways to do this with :code:`asyncio`. If we want to use tasks and be clean about it, we can do it like this:

.. code-block:: python
   :caption: main.py

   from asyncio.tasks import create_task, gather
   from .work import queue_worker_function

   ...
   # We assume that the queues have been initialized already.
   tasks = []
   for _ in range(5):
       new_task = create_task(queue_worker_function(q_in, q_out))
       tasks.append(new_task)
   # Run some other code and let the tasks do their thing.
   ...
   # At some point, we want the tasks to stop waiting for new items and end.
   for task in tasks:
       task.cancel()
   ...
   await gather(*tasks)

By contrast, here is how you would do it with a task pool:

.. code-block:: python
   :caption: main.py

   from asyncio_taskpool import TaskPool
   from .work import queue_worker_function

   ...
   pool = TaskPool()
   group_name = pool.apply(queue_worker_function, args=(q_in, q_out), num=5)
   ...
   pool.cancel_group(group_name)
   ...
   await pool.flush()

Pretty much self-explanatory, no?

Let's consider a slightly more involved example. Assume you have a coroutine function that takes just one argument (some data) as input, does some work with it (maybe connects to the internet in the process), and eventually writes its results to a database (which is globally defined). Here is how that might look:

.. code-block:: python
   :caption: work.py
   :name: another-worker-function

   from .my_database_stuff import insert_into_results_table

   async def another_worker_function(data: object) -> None:
       if data.some_attribute > 1:
           ...
       # Do the work, arrive at results.
       await insert_into_results_table(results)

Say we have some *iterator* of data-items (of arbitrary length) that we want to be worked on, and say we want 5 coroutines concurrently working on that data. Here is a very naive task-based solution:

.. code-block:: python
   :caption: main.py

   from asyncio.tasks import create_task, gather
   from .work import another_worker_function

   async def main():
       ...
       # We got our data_iterator from somewhere.
       keep_going = True
       while keep_going:
           tasks = []
           for _ in range(5):
               try:
                   data = next(data_iterator)
               except StopIteration:
                   keep_going = False
                   break
               new_task = create_task(another_worker_function(data))
               tasks.append(new_task)
           await gather(*tasks)

Here we already run into problems with the task-based approach. The last line in our :code:`while`-loop blocks until **all 5 tasks** return (or raise an exception). This means that as soon as one of them returns, the number of working coroutines is already less than 5 (until all the others return). This can obviously be solved in different ways. We could, for instance, wrap the creation of new tasks itself in a coroutine, which immediately creates a new task, when one is finished, and then call that coroutine 5 times concurrently. Or we could use the queue-based approach from before, but then we would need to write some queue producing coroutine.

Or we could use a task pool:

.. code-block:: python
   :caption: main.py

   from asyncio_taskpool import TaskPool
   from .work import another_worker_function


   async def main():
       ...
       pool = TaskPool()
       pool.map(another_worker_function, data_iterator, num_concurrent=5)
       ...
       await pool.gather_and_close()

Calling the :py:meth:`.map() <asyncio_taskpool.pool.TaskPool.map>` method this way ensures that there will **always**  -- i.e. at any given moment in time -- be exactly 5 tasks working concurrently on our data (assuming no other pool interaction).

The :py:meth:`.gather_and_close() <asyncio_taskpool.pool.BaseTaskPool.gather_and_close>` line will block until **all the data** has been consumed. (see :ref:`blocking-pool-methods`)

.. note::

   Neither :py:meth:`.apply() <asyncio_taskpool.pool.TaskPool.apply>` nor :py:meth:`.map() <asyncio_taskpool.pool.TaskPool.map>` return coroutines. When they are called, the task pool immediately begins scheduling new tasks to run. No :code:`await` needed.

It can't get any simpler than that, can it? So glad you asked...

The :code:`SimpleTaskPool` class
--------------------------------

Let's take the :ref:`queue worker example <queue-worker-function>` from before. If we know that the task pool will only ever work with that one function with the same queue objects, we can make use of the :py:class:`SimpleTaskPool <asyncio_taskpool.pool.SimpleTaskPool>` class:

.. code-block:: python
   :caption: main.py

   from asyncio_taskpool import SimpleTaskPool
   from .work import queue_worker_function


   async def main():
       ...
       pool = SimpleTaskPool(queue_worker_function, args=(q_in, q_out))
       pool.start(5)
       ...
       pool.stop_all()
       ...
       await pool.gather_and_close()

This may, at first glance, not seem like much of a difference, aside from different method names. However, assume that our main function runs a loop and needs to be able to periodically regulate the number of tasks being executed in the pool based on some additional variables it receives. With the :py:class:`SimpleTaskPool <asyncio_taskpool.pool.SimpleTaskPool>`, this could not be simpler:

.. code-block:: python
   :caption: main.py
   :name: simple-control-logic

   from asyncio_taskpool import SimpleTaskPool
   from .work import queue_worker_function


   async def main():
       ...
       pool = SimpleTaskPool(queue_worker_function, args=(q_in, q_out))
       await pool.start(5)
       while True:
          ...
          if some_condition and pool.num_running > 10:
              pool.stop(3)
          elif some_other_condition and pool.num_running < 5:
              pool.start(5)
          else:
              pool.start(1)
          ...
       await pool.gather_and_close()

Notice how we only specify the function and its arguments during initialization of the pool. From that point on, all we need is the :py:meth:`.start() <asyncio_taskpool.pool.SimpleTaskPool.start>` add :py:meth:`.stop() <asyncio_taskpool.pool.SimpleTaskPool.stop>` methods to adjust the number of concurrently running tasks.

The trade-off here is that this simplified task pool class lacks the flexibility of the regular :py:class:`TaskPool <asyncio_taskpool.pool.TaskPool>` class. On an instance of the latter we can call :py:meth:`.map() <asyncio_taskpool.pool.TaskPool.map>` and :py:meth:`.apply() <asyncio_taskpool.pool.TaskPool.apply>` as often as we like with completely unrelated functions and arguments. With a :py:class:`SimpleTaskPool <asyncio_taskpool.pool.SimpleTaskPool>`, once you initialize it, it is pegged to one function and one set of arguments, and all you can do is control the number of tasks working with those.

This simplified interface becomes particularly useful in conjunction with the :doc:`control server <./control>`.

.. _blocking-pool-methods:

(Non-)Blocking pool methods
---------------------------

One of the main concerns when dealing with concurrent programs in general and with :code:`async` functions in particular is when and how a particular piece of code **blocks** during execution, i.e. delays the execution of the following code significantly.

.. note::

   Every statement will block to *some* extent. Obviously, when a program does something, that takes time. This is why the proper question to ask is not *if* but *to what extent, under which circumstances* the execution of a particular line of code blocks.

It is fair to assume that anyone reading this is familiar enough with the concepts of asynchronous programming in Python to know that just slapping :code:`async` in front of a function definition will not magically make it suitable for concurrent execution (in any meaningful way). Therefore, we assume that you are dealing with coroutines that can actually unblock the `event loop  <https://docs.python.org/3/library/asyncio-eventloop.html>`_ (e.g. doing a significant amount of I/O).

So how does the task pool behave in that regard?

The only method of a pool that one should **always** assume to be blocking is :py:meth:`.gather_and_close() <asyncio_taskpool.pool.BaseTaskPool.gather_and_close>`. This method awaits **all** tasks in the pool, meaning as long as one of them is still running, this coroutine will not return.

.. warning::

   This includes awaiting any callbacks that were passed along with the tasks.

One method to be aware of is :py:meth:`.flush() <asyncio_taskpool.pool.BaseTaskPool.flush>`. Since it will await only those tasks that the pool considers **ended** or **cancelled**, the blocking can only come from any callbacks that were provided for either of those situations.

All methods that add tasks to a pool, i.e. :py:meth:`TaskPool.map() <asyncio_taskpool.pool.TaskPool.map>` (and its variants), :py:meth:`TaskPool.apply() <asyncio_taskpool.pool.TaskPool.apply>` and :py:meth:`SimpleTaskPool.start() <asyncio_taskpool.pool.SimpleTaskPool.start>`, are non-blocking by design. They all make use of "meta tasks" under the hood and return immediately. It is important however, to realize that just because they return, does not mean that any actual tasks have been spawned. For example, if a pool size limit was set and there was "no more room" in the pool when :py:meth:`.map() <asyncio_taskpool.pool.TaskPool.map>` was called, there is **no guarantee** that even a single task has started, when it returns.
