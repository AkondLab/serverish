import logging
import asyncio
import datetime

import pytest

from serverish.messenger import Messenger, get_publisher, get_reader, get_callbacksubscriber
from tests.test_connection import ci
from tests.test_nats import is_nats_running, ensure_stram_for_tests



@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_pub_sub_cb():

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S.%f')
    now = datetime.datetime.now()

    pub = get_publisher('test.messenger.test_messenger_pub_sub_cb')
    sub = get_callbacksubscriber('test.messenger.test_messenger_pub_sub_cb', deliver_policy='all')

    msgs = []
    def cb(data, meta):
        logging.debug(f'TEST: Got message {data} {meta}')
        msgs.append(data)
        return True

    async def publisher_task(pub, n):
        for i in range(n):
            await pub.publish(data={'n': i, 'final': False})
            await asyncio.sleep(0.1)

    async with Messenger().context(host='localhost', port=4222) as mess:
        async with sub:
            await mess.purge('test.messenger.test_messenger_pub_sub_cb')
            await publisher_task(pub, 4)
            await asyncio.sleep(0.1)
            await sub.subscribe(cb)
            await sub.wait_for_empty()
            assert len(msgs) == 4
            await asyncio.sleep(1)
            await publisher_task(pub, 5)
            await asyncio.sleep(1)
            await sub.wait_for_empty()
            assert len(msgs) == 9


