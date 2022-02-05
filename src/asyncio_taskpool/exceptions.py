class PoolException(Exception):
    pass


class PoolIsClosed(PoolException):
    pass


class TaskEnded(PoolException):
    pass


class AlreadyCancelled(TaskEnded):
    pass


class AlreadyFinished(TaskEnded):
    pass


class InvalidTaskID(PoolException):
    pass


class PoolStillOpen(PoolException):
    pass
