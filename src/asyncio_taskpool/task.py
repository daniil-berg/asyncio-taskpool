import logging
from asyncio.exceptions import CancelledError
from asyncio.tasks import Task, create_task
from typing import Awaitable, Any

from .types import FinalCallbackT, CancelCallbackT


log = logging.getLogger(__name__)


async def wrap(awaitable: Awaitable, task_name: str, final_callback: FinalCallbackT = None,
               cancel_callback: CancelCallbackT = None) -> Any:
    log.info("Started %s", task_name)
    try:
        return await awaitable
    except CancelledError:
        log.info("Cancelling %s ...", task_name)
        if callable(cancel_callback):
            cancel_callback()
        log.info("Cancelled %s", task_name)
    finally:
        if callable(final_callback):
            final_callback()
        log.info("Exiting %s", task_name)


def start_task(awaitable: Awaitable, task_name: str, final_callback: FinalCallbackT = None,
               cancel_callback: CancelCallbackT = None) -> Task:
    return create_task(wrap(awaitable, task_name, final_callback, cancel_callback), name=task_name)
