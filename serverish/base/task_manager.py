import asyncio
import logging

import param

from serverish.base import Status
from serverish.base.asyncio_util_functions import wait_for_psce
from serverish.base.collector import Collector
from serverish.base.hasstatuses import HasStatuses
from serverish.base.idmanger import IdManager
from serverish.base.singleton import Singleton

logger = logging.getLogger(__name__.rsplit('.')[-1])




class Task(HasStatuses):
    task = param.ClassSelector(default=None, class_=asyncio.Task, allow_None=True, doc='Asyncio task')

    def __init__(self, name, coro, parent: Collector = None, **kwargs) -> None:
        try:
            if not kwargs.pop('i_know_what_i_am_doing'):
                raise RuntimeError('You have to use create_task()')
        except KeyError:
            raise RuntimeError('Use create_task() function to create tasks '
                               '(or, if you realy know what you are doing, pass i_know_what_i_am_doing=True))')


        self.coro = coro
        super().__init__(name=name, parent=parent, **kwargs)
        # self.set_check_methods(ping=self.diagnose_ping, dns=self.diagnose_dns)

    async def start(self):
        """Runs the task. Async - preferred - way"""
        self.start_sync()

    def start_sync(self):
        """Runs the task. Sync method"""
        def done_cb(task):
            try:
                e = task.exception()
            except asyncio.CancelledError:
                logger.debug(f'Task {self.name} canceled')
                self.set_status('running', Status.new_na(msg='Task canceled'))
            else:
                if e is not None:
                    logger.error(f'Task {self.name} failed: {task.exception()}', exc_info=task.exception())
                    self.set_status('running', Status.new_fail(msg=f'Task failed: {task.exception()}'))
                else:
                    logger.debug(f'Task {self.name} done')
                    self.set_status('running', Status.new_na(msg='Task finished'))
            self.remove_parent()

        self.task = asyncio.create_task(self.coro, name=self.name)
        self.task.add_done_callback(done_cb)

    def cancel(self):
        """Cancels the task"""
        if self.task is not None:
            self.task.cancel()

    def __await__(self):
        """Awaiting task"""
        return self.task.__await__()

    def done(self):
        """Returns True if task is done"""
        return self.task.done()

    def cancelled(self):
        """Returns True if task is canceled"""
        return self.task.cancelled()

    def result(self):
        """Returns task result"""
        return self.task.result()

    def exception(self):
        """Returns task exception"""
        return self.task.exception()

    async def wait_for(self, timeout: float = None, cancel_on_timeout: bool = True):
        """Waits for task to finish, with timeout

        If timeout passed, task is canceled and Timeout exception is raised
        If cancel_on_timeout is False, task is not canceled after timeout, but method returns"""

        if not self.task.done():
            if not cancel_on_timeout:
                t = asyncio.shield(self.task)
            else:
                t = self.task
            try:
                await asyncio.wait_for(t, timeout=timeout)
            except asyncio.TimeoutError:
                logger.debug(f'wait_for Task {self.name} timeout (task canceling:{cancel_on_timeout})')
                raise
            except asyncio.CancelledError:
                logger.debug(f'wait_for Task {self.name} canceled (task canceling:{cancel_on_timeout})')
                raise


async def create_task(coro, name: str, class_=Task) -> Task:
    """Creates task, like asyncio.create_task, but tracked. Async - preferred - way"""
    tm = TaskManager()
    return await tm.create_task(coro, name=name, class_=class_)

def create_task_sync(coro, name: str, class_=Task) -> Task:
    """Creates task, like asyncio.create_task, but tracked"""
    tm = TaskManager()
    return tm.create_task_sync(coro, name=name, class_=class_)


class TaskManager(Singleton):
    """Manages running asyncio tasks

    Done tasks removes themselves from manager"""

    async def create_task(self, coro, name: str, class_=Task) -> Task:
        """Creates task, like asyncio.create_task, but tracked. Async - preferred - way"""
        task = class_(coro=coro, name=name, parent=self, i_know_what_i_am_doing=True)
        await task.start()
        return task

    def create_task_sync(self, coro, name: str, class_=Task) -> Task:
        """Creates task, like asyncio.create_task, but tracked"""
        task = class_(coro=coro, name=name, parent=self, i_know_what_i_am_doing=True)
        task.start_sync()
        return task

    async def cancel_all(self, timeout: float = 10):
        """Cancels all tasks"""
        n = 0
        tasks = [t for t in self.children]
        for task in tasks:
            n += 1
            try:
                t: asyncio.Task = task.task
                t.cancel()
            except RuntimeError as e:
                logger.error(f'Error canceling task {n} {task.name}: {e}')

        logger.info('Canceled all tasks. Waiting for them to finish...')

        for task in tasks:
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
        for task in tasks:
            if not task.task.done():
                logger.error(f'Task {task.name} still not done (after cancel all).')
                nf.append(task)

        if nf:
            logger.error(f'Not finished tasks: {[task.name for task in nf]}')
        else:
            logger.info(f'All tasks finished.')

