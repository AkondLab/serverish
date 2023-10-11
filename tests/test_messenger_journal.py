"""Tests for the journaling features of the messenger module, provided by the
`serverish.messenger.msg_journal_pub` and `serverish.messenger.msg_journal_read` modules.
"""
import asyncio

import pytest

from serverish.base import MessengerRequestNoResponders, MessengerRequestJetStreamSubject
from serverish.messenger import Messenger, get_journalreader, get_journalpublisher
from tests.test_connection import ci
from tests.test_nats import is_nats_running



@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_obtaining_reader():
    subject = 'test.messenger.test_messenger_obtaining_reader'

    async with Messenger().context(host='localhost', port=4222) as mess:
        writer = get_journalpublisher(subject)
        assert writer.subject == subject
        reader = get_journalreader(subject)
        assert reader.subject == subject

@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_publishing():
    subject = 'test.messenger.test_messenger_publishing'

    async with Messenger().context(host='localhost', port=4222) as mess:
        await mess.purge(subject)
        publisher = get_journalpublisher(subject)
        await publisher.info('test info: hello %s', 'world')
        await publisher.warning('test warning: hello %s', 'world')
        await publisher.error('test error: hello %s', 'world')
        await publisher.critical('test critical: hello %s', 'world')
        await publisher.debug('test debug: hello %s', 'world')
        await publisher.notice('test exception: hello %s', 'world')


@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_publishing_timeit():
    subject = 'test.messenger.test_messenger_publishing_timeit'

    async with Messenger().context(host='localhost', port=4222) as mess:
        await mess.purge(subject)
        publisher = get_journalpublisher(subject)
        # calc the time to publish number of messages
        n = 200
        start = asyncio.get_running_loop().time()
        for i in range(n):
            # {'trace_level': 0} prevents the message from being logged
            await publisher.info('test info: hello %s', 'world', meta={'trace_level': 0})
        t = asyncio.get_running_loop().time() - start

        print (f'\nTime to publish 1 message: {1000.0*t/n:.2f}ms')


@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_pub_then_read_and_pub():
    subject = 'test.messenger.test_messenger_pub_then_read_and_pub'

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


    async with Messenger().context(host='localhost', port=4222) as mess:
        await mess.purge(subject)
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



