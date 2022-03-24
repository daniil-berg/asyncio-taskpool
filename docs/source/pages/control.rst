.. This file is part of asyncio-taskpool.

.. asyncio-taskpool is free software: you can redistribute it and/or modify it under the terms of
   version 3.0 of the GNU Lesser General Public License as published by the Free Software Foundation.

.. asyncio-taskpool is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
   without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
   See the GNU Lesser General Public License for more details.

.. You should have received a copy of the GNU Lesser General Public License along with asyncio-taskpool.
   If not, see <https://www.gnu.org/licenses/>.

.. Copyright © 2022 Daniil Fajnberg

Control interface
=================

When you are dealing with programs that run for a long period of time or even as daemons (i.e. indefinitely), having a way to adjust their behavior without needing to stop and restart them can be desirable.

Task pools offer a high degree of flexibility regarding the number and kind of tasks that run within them, by providing methods to easily start and stop tasks and task groups. But without additional tools, they only allow you to establish a control logic *a priori*, as demonstrated in :ref:`this code snippet <simple-control-logic>`.

What if you have a long-running program that executes certain tasks concurrently, but you don't know in advance how many of them you'll need? What if you want to be able to adjust the number of tasks manually **without stopping the task pool**?

The control server
------------------

The :code:`asyncio-taskpool` library comes with a simple control interface for managing task pools that are already running, at the heart of which is the :py:class:`ControlServer <asyncio_taskpool.control.server.ControlServer>`. Any task pool can be passed to a control server. Once the server is running, you can issue commands to it either via TCP or via UNIX socket. The commands map directly to the task pool methods.

To enable control over a :py:class:`SimpleTaskPool <asyncio_taskpool.pool.SimpleTaskPool>` via local TCP port :code:`8001`, all you need to do is this:

.. code-block:: python
   :caption: main.py
   :name: control-server-minimal

   from asyncio_taskpool import SimpleTaskPool
   from asyncio_taskpool.control import TCPControlServer
   from .work import any_worker_func


   async def main():
       ...
       pool = SimpleTaskPool(any_worker_func, kwargs={'foo': 42, 'bar': some_object})
       control = await TCPControlServer(pool, host='127.0.0.1', port=8001).serve_forever()
       await control

Under the hood, the :py:class:`ControlServer <asyncio_taskpool.control.server.ControlServer>` simply uses :code:`asyncio.start_server` for instantiating a socket server. The resulting control task will run indefinitely. Cancelling the control task stops the server.

In reality, you would probably want some graceful handler for an interrupt signal that cancels any remaining tasks as well as the serving control task.

The control client
------------------

Technically, any process that can read from and write to the socket exposed by the control server, will be able to interact with it. The :code:`asyncio-taskpool` package has its own simple implementation in the form of the :py:class:`ControlClient <asyncio_taskpool.control.client.ControlClient>` that makes it easy to use out of the box.

To start a client, you can use the main script of the :py:mod:`asyncio_taskpool.control` sub-package like this:

.. code-block:: bash

   $ python -m asyncio_taskpool.control tcp localhost 8001

This would establish a connection to the control server from the previous example. Calling

.. code-block:: bash

   $ python -m asyncio_taskpool.control -h

will display the available client options.

The control session
-------------------

Assuming you connected successfully, you should be greeted by the server with a help message and dropped into a simple input prompt.

.. code-block:: none

   Connected to SimpleTaskPool-0
   Type '-h' to get help and usage instructions for all available commands.

   >

The input sent to the server is handled by a typical argument parser, so the interface should be straight-forward. A command like

.. code-block:: none

   > start 5

will call the :py:meth:`.start() <asyncio_taskpool.pool.SimpleTaskPool.start>` method with :code:`5` as an argument and thus start 5 new tasks in the pool, while the command

.. code-block:: none

   > pool-size

will call the :py:meth:`.pool_size <asyncio_taskpool.pool.BaseTaskPool.pool_size>` property getter and return the maximum number of tasks you that can run in the pool.

When you are dealing with a regular :py:class:`TaskPool <asyncio_taskpool.pool.TaskPool>` instance, starting new tasks works just fine, as long as the coroutine functions you want to use can be imported into the namespace of the pool. If you have a function named :code:`worker` in the module :code:`mymodule` under the package :code:`mypackage` and want to use it in a :py:meth:`.map() <asyncio_taskpool.pool.TaskPool.map>` call with the arguments :code:`'x'`, :code:`'x'`, and :code:`'z'`, you would do it like this:

.. code-block:: none

   > map mypackage.mymodule.worker ['x','y','z'] -g 3

The :code:`-g` is a shorthand for :code:`--group-size` in this case. In general, all (public) pool methods will have a corresponding command in the control session.

.. note::

   The :code:`ast.literal_eval` function from the `standard library <https://docs.python.org/3/library/ast.html#ast.literal_eval>`_ is used to safely evaluate the iterable of arguments to work on. For obvious reasons, being able to provide arbitrary python objects in such a control session is neither practical nor secure. The way this is implemented now is limited in that regard, since you can only use Python literals and containers as arguments for your coroutine functions.

To exit a control session, use the :code:`exit` command or simply press :code:`Ctrl + D`.
