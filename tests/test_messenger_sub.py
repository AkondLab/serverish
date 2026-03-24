import logging
import asyncio
import datetime

import pytest

from serverish.messenger import Messenger, get_publisher, get_reader, get_callbacksubscriber


@pytest.mark.nats
async def test_messenger_pub_sub_cb(messenger, unique_subject):

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S.%f')
    now = datetime.datetime.now()

    msgs = []
    def cb(data, meta):
        print(data)
        msgs.append(data)
        return True

    async def publisher_task(pub, n):
        for i in range(n):
            await pub.publish(data={'n': i, 'final': False})
            await asyncio.sleep(0.1)

    pub = get_publisher(subject=unique_subject)
    await messenger.purge(subject=unique_subject)
    await publisher_task(pub, 4)
    await asyncio.sleep(0.1)
    sub = get_callbacksubscriber(subject=unique_subject, deliver_policy='all')
    async with sub:
        await sub.subscribe(cb)
        await sub.wait_for_empty()
        assert len(msgs) == 4
        await asyncio.sleep(1)
        await publisher_task(pub, 5)
        await asyncio.sleep(6)
        await sub.wait_for_empty()
        assert len(msgs) == 9
