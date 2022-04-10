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

This module should **not** be considered part of the public API.
"""


import sys


PACKAGE_NAME = 'asyncio_taskpool'

PYTHON_BEFORE_39 = sys.version_info[:2] < (3, 9)

DEFAULT_TASK_GROUP = 'default'

SESSION_MSG_BYTES = 1024 * 100

CMD = 'command'
CMD_OK = b"ok"


class CLIENT_INFO:
    __slots__ = ()
    TERMINAL_WIDTH = 'terminal_width'
