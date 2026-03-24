import logging
import asyncio
import datetime

import pytest

from serverish.messenger import Messenger, get_publisher, get_reader, get_callbacksubscriber


@pytest.mark.nats
async def test_messenger_progress(messenger, unique_subject):
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S.%f')
    now = datetime.datetime.now()

    # pub = get_publisher(unique_subject)
    # sub = get_callbacksubscriber(unique_subject, deliver_policy='all')
    #
    # msgs = []
    # def cb(data, meta):
    #     print(data, sub)
    #     msgs.append(data)
    #     return True
    #
    # async def publisher_task(pub, n):
    #     for i in range(n):
    #         await pub.publish(data={'n': i, 'final': False})
    #         await asyncio.sleep(0.1)
    #
    # async with sub:
    #     await messenger.purge(unique_subject)
    #     await publisher_task(pub, 4)
    #     await asyncio.sleep(0.1)
    #     await sub.subscribe(cb)
    #     await sub.wait_for_empty()
    #     assert len(msgs) == 4
    #     await asyncio.sleep(1)
    #     await publisher_task(pub, 5)
    #     await asyncio.sleep(1)
    #     await sub.wait_for_empty()
    #     assert len(msgs) == 9
    #
    pass
