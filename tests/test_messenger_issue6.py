import asyncio

import nats
import pytest

from serverish.messenger import Messenger, get_reader, get_publisher
from tests.test_connection import ci
from tests.test_nats import is_nats_running


@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_issue6_new_fails():

    subject = 'test.messenger.test_messenger_issue6_new_fails'

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

    async with Messenger().context(host='localhost', port=4222) as mes:
        await mes.purge(subject)
        pub = get_publisher(subject=subject)
        # Exception when reader deliver_policy is 'new' is a subject of the issue 6
        sub = get_reader(subject=subject, deliver_policy='new')
        # sub = get_reader(subject=subject, deliver_policy='by_start_time', opt_start_time=now)
        await asyncio.gather(subsciber_task(sub), publisher_task(pub))
        await pub.close()
        await sub.close()

