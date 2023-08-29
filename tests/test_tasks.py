import asyncio
from random import random

import pytest

from serverish.base import create_task, TaskManager

@pytest.mark.asyncio
async def test_tasks_cancel_all():
    async def task():
        while True:
            await asyncio.sleep(10*random())

    for i in range(100):
        await create_task(task(), 'test_tasks_cancel_all')
    await asyncio.sleep(0.1)

    tm = TaskManager()
    await tm.cancel_all()
    for task in tm.children:
        assert task.task.done()
