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
Custom exception classes used in various modules.
"""


class PoolException(Exception):
    pass


class PoolIsClosed(PoolException):
    pass


class PoolIsLocked(PoolException):
    pass


class TaskEnded(PoolException):
    pass


class AlreadyCancelled(TaskEnded):
    pass


class AlreadyEnded(TaskEnded):
    pass


class InvalidTaskID(PoolException):
    pass


class InvalidGroupName(PoolException):
    pass


class NotCoroutine(PoolException):
    pass


class ServerException(Exception):
    pass


class HelpRequested(ServerException):
    pass


class ParserError(ServerException):
    pass


class CommandError(ServerException):
    pass
