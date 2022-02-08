class PoolException(Exception):
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


class PoolStillUnlocked(PoolException):
    pass


class NotCoroutine(PoolException):
    pass
