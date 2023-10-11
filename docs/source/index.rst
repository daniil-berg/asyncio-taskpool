.. This file is part of asyncio-taskpool.

.. asyncio-taskpool is free software: you can redistribute it and/or modify it under the terms of
   version 3.0 of the GNU Lesser General Public License as published by the Free Software Foundation.

.. asyncio-taskpool is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
   without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
   See the GNU Lesser General Public License for more details.

.. You should have received a copy of the GNU Lesser General Public License along with asyncio-taskpool.
   If not, see <https://www.gnu.org/licenses/>.

.. Copyright © 2023 Daniil Fajnberg


Welcome to the asyncio-taskpool documentation!
==============================================

:code:`asyncio-taskpool` is a Python library for dynamically and conveniently managing pools of `asyncio <https://docs.python.org/3/library/asyncio.html>`_ tasks.

Purpose
-------

A `task <https://docs.python.org/3/library/asyncio-task.html>`_ is a very powerful tool of concurrency in the Python world. Since concurrency always implies doing more than one thing at a time, you rarely deal with just one :code:`Task` instance. However, managing multiple tasks can become a bit cumbersome quickly, as their number increases. Moreover, especially in long-running code, you may find it useful (or even necessary) to dynamically adjust the extent to which the work is distributed, i.e. increase or decrease the number of tasks.

With that in mind, this library aims to provide two things:

#. An additional layer of abstraction and convenience for managing multiple tasks.
#. A simple interface for dynamically adding and removing tasks when a program is already running.

The first is achieved through the concept of a :doc:`task pool <pages/pool>`. The second is achieved by adding a :doc:`control server <pages/control>` to the task pool.

Installation
------------

.. code-block:: bash

   $ pip install asyncio-taskpool


Contents
--------

.. toctree::
   :maxdepth: 2

   pages/pool
   pages/ids
   pages/control
   api/api


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
