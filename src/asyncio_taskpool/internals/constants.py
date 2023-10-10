"""
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
