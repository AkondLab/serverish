import asyncio
from random import random

import pytest

from serverish.base import create_task, TaskManager, Task


@pytest.mark.asyncio
async def test_tasks_cancel_all():
    async def task():
        try:
            while True:
                await asyncio.sleep(10*random())
        except asyncio.CancelledError:
            pass

    for i in range(100):
        await create_task(task(), 'test_tasks_cancel_all')
    await asyncio.sleep(2)

    tm = TaskManager()
    await tm.cancel_all()
    await asyncio.sleep(2)

    for task in tm.children:
        assert task.task.done()

def test_tasks_cancel_all_s():
    asyncio.run(test_tasks_cancel_all())

def test_tasks_direct_constructor_call():
    """ You should not directly call Task() constructor, or probide `i_know_what_i_am_doing=True`:"""
    with pytest.raises(RuntimeError):
        Task('test', None)
    with pytest.raises(RuntimeError):
        Task('test', None, i_know_what_i_am_doing=False)
    Task('test', None, i_know_what_i_am_doing=True)

def test_task_await_method():
    """ Tests awaiting serverish.base.Task object to check if the `__await__` method is implemented properly"""
    async def task():
        await asyncio.sleep(0.1)
        return 1

    async def test():
        t = await create_task(task(), 'test_task_await_method')
        assert await t == 1

    asyncio.run(test())


async def _task(sleep):
    await asyncio.sleep(sleep)
    return 1


def test_task_wait_for_method_timeout_none():
    """ Tests serverish.base.Task.wait_for() method"""
    async def test():
        t = await create_task(_task(0.1), 'test_task_wait_for_method')
        await t.wait_for()
        assert t.done()
        assert not t.cancelled()
        assert t.result() == 1
        assert await t == 1

    asyncio.run(test())

def test_task_wait_for_method_timeouted():
    """ Tests serverish.base.Task.wait_for() method"""
    async def test():
        t = await create_task(_task(0.1), 'test_task_wait_for_method')
        with pytest.raises(asyncio.TimeoutError):
            await t.wait_for(timeout=0.05)
        assert t.done()
        assert t.cancelled()
        with pytest.raises(asyncio.CancelledError):
            t.result()
        with pytest.raises(asyncio.CancelledError):
            await t

    asyncio.run(test())

def test_task_wait_for_method_timeouted_no_cancel():
    """ Tests serverish.base.Task.wait_for() method"""
    async def test():
        t = await create_task(_task(0.1), 'test_task_wait_for_method')
        with pytest.raises(asyncio.TimeoutError):
            await t.wait_for(timeout=0.05, cancel_on_timeout=False)
        # Task is still working
        assert not t.task.done()
        with pytest.raises(asyncio.InvalidStateError):
            t.result()
        assert await t == 1
        # Task is finished
        assert t.done()
        assert t.result() == 1

    asyncio.run(test())