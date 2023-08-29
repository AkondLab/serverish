import asyncio
import logging

import param

from serverish.base.asyncio_util_functions import wait_for_psce
from serverish.base.collector import Collector
from serverish.base.hasstatuses import HasStatuses
from serverish.base.idmanger import IdManager
from serverish.base.singleton import Singleton

logger = logging.getLogger(__name__.rsplit('.')[-1])


def create_task(coro, name: str):
    """Creates task, like asyncio.create_task, but tracked"""
    tm = TaskManager()
    return tm.create_task(coro, name=name)


class Task(HasStatuses):
    task = param.ClassSelector(default=None, class_=asyncio.Task, allow_None=True, doc='Asyncio task')

    def __init__(self, name, coro, parent: Collector = None, **kwargs) -> None:
        self.coro = coro
        super().__init__(name=name, parent=parent, **kwargs)
        # self.set_check_methods(ping=self.diagnose_ping, dns=self.diagnose_dns)

    async def start(self):
        """Runs the task"""
        self.task = asyncio.create_task(self.coro, name=self.name)


class TaskManager(Singleton):
    """Manages asyncio tasks"""

    async def create_task(self, coro, name: str):
        """Creates task, like asyncio.create_task, but tracked"""
        task = Task(coro=coro, name=name, parent=self)
        await task.start()
        return task

    async def cancel_all(self, timeout: float = 10):
        """Cancels all tasks"""
        for task in self.children:
            t: asyncio.Task = task.task
            t.cancel()

        logger.info('Canceled all tasks. Waiting for them to finish...')

        for task in self.children:
            try:
                await wait_for_psce(task.task, timeout)
            except asyncio.TimeoutError:
                logger.error(f'Timeout during all tasks cancellation.')
            except asyncio.CancelledError:
                if asyncio.current_task().cancelled():
                    logger.error(f'"Canceling Tasks" task canceled....')
                    raise

        logger.info('All tasks finished? Checking')

        # check (optional)
        nf = []
        for task in self.children:
            if not task.task.done():
                logger.error(f'Task {task.name} still not done (after cancel all).')
                nf.append(task)

        if nf:
            logger.error(f'Not finished tasks: {[task.name for task in nf]}')
        else:
            logger.info(f'All tasks finished.')

