"""Custom exception classes used in various modules."""


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


# ruff: noqa: D101
