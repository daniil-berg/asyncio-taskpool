class PoolException(Exception):
    pass


class PoolIsClosed(PoolException):
    pass


class TaskEnded(PoolException):
    pass


class AlreadyCancelled(TaskEnded):
    pass


class AlreadyEnded(TaskEnded):
    pass


class InvalidTaskID(PoolException):
    pass


class PoolStillOpen(PoolException):
    pass


class NotCoroutine(PoolException):
    pass
