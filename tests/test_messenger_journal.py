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


@pytest.mark.nats
async def test_messenger_pub_then_read_and_pub(messenger, unique_subject):
    subject = unique_subject

    collected = []

    async def publisher_task(pub, n, stop):
        for i in range(n):
            meta = {}
            if stop and i == n-1:
                meta['tags'] = ['stop']
            await pub.info('test info: hello %s', 'world', meta=meta)
            await asyncio.sleep(0.1)

    async def reader_task(reader):
        async for entry, meta in reader:
            collected.append(entry)
            if 'stop' in meta.get('tags', []):
                break


    await messenger.purge(subject)
    publisher = get_journalpublisher(subject)
    reader = get_journalreader(subject, deliver_policy='all')
    # prepublish some messages
    n = 10
    await publisher_task(publisher, n, False)
    # start the reader
    reader_task = asyncio.create_task(reader_task(reader))
    # publish some more messages
    await publisher_task(publisher, n, True)
    # wait for the reader to finish
    await reader_task
    # check the collected messages
    assert len(collected) == 2*n


