"""Tests for the journaling features of the messenger module, provided by the
`serverish.messenger.msg_journal_pub` and `serverish.messenger.msg_journal_read` modules.
"""
import asyncio
import logging

import pytest

from serverish.base import MessengerRequestNoResponders, MessengerRequestJetStreamSubject
from serverish.messenger import Messenger, get_journalreader, get_journalpublisher



@pytest.mark.nats
async def test_messenger_obtaining_reader(messenger, unique_subject):
    subject = unique_subject

    writer = get_journalpublisher(subject)
    assert writer.subject == subject
    reader = get_journalreader(subject)
    assert reader.subject == subject

@pytest.mark.nats
async def test_messenger_publishing(messenger, unique_subject):
    subject = unique_subject

    await messenger.purge(subject)
    publisher = get_journalpublisher(subject)
    await publisher.info('test info: hello %s', 'world')
    await publisher.warning('test warning: hello %s', 'world')
    await publisher.error('test error: hello %s', 'world')
    await publisher.critical('test critical: hello %s', 'world')
    await publisher.debug('test debug: hello %s', 'world')
    await publisher.notice('test exception: hello %s', 'world')



@pytest.mark.skip(reason="Slow test - manual debugging only")
@pytest.mark.nats
async def test_messenger_publishing_slow(messenger, unique_subject):
    subject = unique_subject

    await messenger.purge(subject)
    publisher = get_journalpublisher(subject)
    for i in range(100):
        try:
            await publisher.info('Message %d', i)
        except Exception as e:
            logging.error(f"Failed to publish message {i}, Exception {type(e)}: {e}")
        else:
            logging.info(f"Published message {i}")
        await asyncio.sleep(1.0)



@pytest.mark.nats
async def test_messenger_publishing_timeit(messenger, unique_subject):
    subject = unique_subject

    await messenger.purge(subject)
    publisher = get_journalpublisher(subject)
    # calc the time to publish number of messages
    n = 200
    start = asyncio.get_running_loop().time()
    for i in range(n):
        # {'trace_level': 0} prevents the message from being logged
        await publisher.info('test info: hello %s', 'world', meta={'trace_level': 0})
    t = asyncio.get_running_loop().time() - start

    print (f'\nTime to publish 1 message: {1000.0*t/n:.2f}ms')



