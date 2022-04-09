.. This file is part of asyncio-taskpool.

.. asyncio-taskpool is free software: you can redistribute it and/or modify it under the terms of
   version 3.0 of the GNU Lesser General Public License as published by the Free Software Foundation.

.. asyncio-taskpool is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
   without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
   See the GNU Lesser General Public License for more details.

.. You should have received a copy of the GNU Lesser General Public License along with asyncio-taskpool.
   If not, see <https://www.gnu.org/licenses/>.

.. Copyright © 2022 Daniil Fajnberg


IDs, groups & names
===================

Task IDs
--------

Every task spawned within a pool receives an ID, which is an integer greater or equal to 0 that is unique **within that task pool instance**. An internal counter is incremented whenever a new task is spawned. A task with ID :code:`n` was the :code:`(n+1)`-th task to be spawned in the pool. Task IDs can be used to cancel specific tasks using the :py:meth:`.cancel() <asyncio_taskpool.pool.BaseTaskPool.cancel>` method.

In practice, it should rarely be necessary to target *specific* tasks. When dealing with a regular :py:class:`TaskPool <asyncio_taskpool.pool.TaskPool>` instance, you would typically cancel entire task groups (see below) rather than individual tasks, whereas with :py:class:`SimpleTaskPool <asyncio_taskpool.pool.SimpleTaskPool>` instances you would indiscriminately cancel a number of tasks using the :py:meth:`.stop() <asyncio_taskpool.pool.SimpleTaskPool.stop>` method.

The ID of a pool task also appears in the task's name, which is set upon spawning it. (See `here <https://docs.python.org/3/library/asyncio-task.html#asyncio.Task.set_name>`_ for the associated method of the :code:`Task` class.)

Task groups
-----------

Every method of spawning new tasks in a task pool will add them to a **task group** and return the name of that group. With :py:class:`TaskPool <asyncio_taskpool.pool.TaskPool>` methods such as :py:meth:`.apply() <asyncio_taskpool.pool.TaskPool.apply>` and :py:meth:`.map() <asyncio_taskpool.pool.TaskPool.map>`, the group name can be set explicitly via the :code:`group_name` parameter. By default, the name will be a string containing some meta information depending on which method is used. Passing an existing task group name in any of those methods will result in a :py:class:`InvalidGroupName <asyncio_taskpool.exceptions.InvalidGroupName>` error.

You can cancel entire task groups using the :py:meth:`.cancel_group() <asyncio_taskpool.pool.BaseTaskPool.cancel_group>` method by passing it the group name. To check which tasks belong to a group, the :py:meth:`.get_group_ids() <asyncio_taskpool.pool.BaseTaskPool.get_group_ids>` method can be used, which takes group names and returns the IDs of the tasks belonging to them.

The :py:meth:`SimpleTaskPool.start() <asyncio_taskpool.pool.SimpleTaskPool.start>` method will create a new group as well, each time it is called, but it does not allow customizing the group name. Typically, it will not be necessary to keep track of groups in a :py:class:`SimpleTaskPool <asyncio_taskpool.pool.SimpleTaskPool>` instance.

Task groups do not impose limits on the number of tasks in them, although they can be indirectly constrained by pool size limits.

Pool names
----------

When initializing a task pool, you can provide a custom name for it, which will appear in its string representation, e.g. when using it in a :code:`print()`. A class attribute keeps track of initialized task pools and assigns each one an index (similar to IDs for pool tasks). If no name is specified when creating a new pool, its index is used in the string representation of it. Pool names can be helpful when using multiple pools and analyzing log messages.
