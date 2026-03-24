import asyncio

import nats
import pytest

from serverish.messenger import Messenger, get_reader, get_publisher


@pytest.mark.nats
async def test_messenger_issue6_new_fails(messenger, unique_subject):

    async def subsciber_task(sub):
        async for data, meta in sub:
            print(data)
            if data['final']:
                break

    async def publisher_task(pub):
        await asyncio.sleep(0.1)
        for i in range(10):
            await pub.publish(data={'n': i, 'final': False})
            await asyncio.sleep(0.05)
        await pub.publish(data={'n': 10, 'final': True})

    await messenger.purge(unique_subject)
    pub = get_publisher(subject=unique_subject)
    # Exception when reader deliver_policy is 'new' is a subject of the issue 6
    sub = get_reader(subject=unique_subject, deliver_policy='new')
    # sub = get_reader(subject=unique_subject, deliver_policy='by_start_time', opt_start_time=now)
    await asyncio.gather(subsciber_task(sub), publisher_task(pub))
    await pub.close()
    await sub.close()
