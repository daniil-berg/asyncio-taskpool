__author__ = "Daniil Fajnberg"
__copyright__ = "Copyright © 2022 Daniil Fajnberg"
__license__ = """GNU LGPLv3.0

This file is part of asyncio-taskpool.

asyncio-taskpool is free software: you can redistribute it and/or modify it under the terms of
version 3.0 of the GNU Lesser General Public License as published by the Free Software Foundation.

asyncio-taskpool is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 
See the GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License along with asyncio-taskpool. 
If not, see <https://www.gnu.org/licenses/>."""

__doc__ = """
Constants used by more than one module in the package.
"""


PACKAGE_NAME = 'asyncio_taskpool'

DEFAULT_TASK_GROUP = ''
DATETIME_FORMAT = '%Y-%m-%d_%H-%M-%S'

CLIENT_EXIT = 'exit'

SESSION_MSG_BYTES = 1024 * 100
SESSION_WRITER = 'session_writer'


class CLIENT_INFO:
    __slots__ = ()
    TERMINAL_WIDTH = 'terminal_width'


class CMD:
    __slots__ = ()
    CMD = 'command'
    NAME = 'name'
    POOL_SIZE = 'pool-size'
    NUM_RUNNING = 'num-running'
    START = 'start'
    STOP = 'stop'
    STOP_ALL = 'stop-all'
    FUNC_NAME = 'func-name'
