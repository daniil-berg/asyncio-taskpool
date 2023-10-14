"""Custom exception classes used in various modules."""

from __future__ import annotations

from typing import TYPE_CHECKING, Awaitable, Callable

if TYPE_CHECKING:
    from .pool import BaseTaskPool


class PoolException(Exception):
    pass


class PoolIsClosed(PoolException):
    pass


class PoolIsLocked(PoolException):
    pass


class TaskEnded(PoolException):
    pass


class AlreadyCancelled(TaskEnded):
    def __init__(self, task_name: str) -> None:
        """Includes the `task_name` in the error message."""
        super().__init__(f"'{task_name}' has been cancelled")


class AlreadyEnded(TaskEnded):
    def __init__(self, task_name: str) -> None:
        """Includes the `task_name` in the error message."""
        super().__init__(f"'{task_name}' has finished running")


class InvalidTaskID(PoolException):
    pass


class TaskNotFound(InvalidTaskID):
    def __init__(self, task_id: int, pool: BaseTaskPool) -> None:
        """Includes the `task_id` and `pool` in the error message."""
        super().__init__(f"No task with ID {task_id} found in {pool}")


class InvalidGroupName(PoolException):
    pass


class TaskGroupNotFound(InvalidGroupName):
    def __init__(self, name: str) -> None:
        """Includes the `name` of the task group in the error message."""
        super().__init__(f"No task group named '{name}' exists in this pool.")


class TaskGroupAlreadyExists(InvalidGroupName):
    def __init__(self, name: str) -> None:
        """Includes the `name` of the task group in the error message."""
        super().__init__(f"Task group named '{name}' already exists.")


class NotCoroutine(PoolException):
    def __init__(
        self,
        coroutine: Awaitable[object],
    ) -> None:
        """Includes the `coroutine` at the end of the error message."""
        super().__init__(f"Not a coroutine: {coroutine}")


class NotCoroutineFunction(NotCoroutine):
    def __init__(
        self,
        function: Callable[..., Awaitable[object]],
    ) -> None:
        """Includes the `function` at the end of the error message."""
        super(PoolException, self).__init__(
            f"Not a coroutine function: {function}"
        )


class ServerException(Exception):
    pass


class ServerNotInitialized(ServerException):
    pass


class HelpRequested(ServerException):
    pass


class ParserError(ServerException):
    pass


class ParserNotInitialized(ParserError):
    pass


class SubParsersNotInitialized(ParserError):
    pass


class CommandError(ServerException):
    pass


# ruff: noqa: D101
