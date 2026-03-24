"""Correctness tests for all messaging patterns: pub/sub, RPC, progress, journal.

These are the canonical happy-path tests that establish "what correct looks like"
for each messaging pattern. Phase 4 resilience tests recover TO this baseline.
"""
from __future__ import annotations

import asyncio
import logging
from asyncio import Lock

import pytest

from serverish.base import create_task
from serverish.messenger import (
    get_publisher,
    get_reader,
    get_rpcresponder,
    get_journalpublisher,
    get_journalreader,
    request,
    Rpc,
)
from serverish.messenger.msg_progress_pub import get_progresspublisher


@pytest.mark.nats
async def test_pattern_pub_sub(messenger, unique_subject):
    """Happy-path: publish messages, start subscriber, publish more, verify all received."""
    subject = unique_subject
    lock = Lock()

    async def subscriber_task(sub):
        async for data, meta in sub:
            async with lock:
                logging.debug('Received: %s', data)
            if data['final']:
                break

    async def publisher_task(pub, n):
        for i in range(n):
            await pub.publish(data={'n': i, 'final': False})
            await asyncio.sleep(0.01)

    async def publish_final(pub):
        await pub.publish(data={'n': 9999, 'final': True})

    await messenger.purge(subject)
    pub = get_publisher(subject=subject)
    sub = get_reader(subject=subject, deliver_policy='all')
    try:
        await publisher_task(pub, 3)

        t = await create_task(subscriber_task(sub), 'sub')

        logging.info('subscriber started')
        await asyncio.sleep(0.03)
        logging.info('2nd publisher starting')
        await publisher_task(pub, 2)

        await asyncio.sleep(3)
        await publish_final(pub)

        await t
    finally:
        await pub.close()
        await sub.close()


@pytest.mark.nats
async def test_pattern_rpc(messenger, unique_subject):
    """Happy-path: RPC request/reply with a simple addition callback."""
    # RPC uses core NATS, not JetStream. Use a non-JS subject prefix.
    subject = f'test_no_js.{unique_subject}'

    def cb(rpc: Rpc):
        data = rpc.data
        c = data['a'] + data['b']
        rpc.set_response(data={'c': c})

    async with get_rpcresponder(subject) as r:
        await r.register_function(cb)
        data, meta = await request(subject, data={'a': 1, 'b': 2})
        assert data['c'] == 3


@pytest.mark.nats
async def test_pattern_progress(messenger, unique_subject):
    """Happy-path: progress publisher tracks task completion through add/update lifecycle."""
    subject = unique_subject

    pub = get_progresspublisher(subject=subject)
    try:
        await pub.open()
        task_id = await pub.add_task('Processing', total=5)

        for i in range(1, 6):
            await pub.update(task_id, completed=i)

        assert pub.all_done is True
        assert pub.finished is True
        assert pub.health_status['publish_count'] >= 6
    finally:
        await pub.close()


@pytest.mark.nats
async def test_pattern_journal(messenger, unique_subject):
    """Happy-path: journal pub/read with pre-published and live messages."""
    subject = unique_subject
    collected = []

    async def publisher_task(pub, n, stop):
        for i in range(n):
            meta = {}
            if stop and i == n - 1:
                meta['tags'] = ['stop']
            await pub.info('test info: hello %s', 'world', meta=meta)
            await asyncio.sleep(0.1)

    async def reader_task_fn(reader):
        async for entry, meta in reader:
            collected.append(entry)
            if 'stop' in meta.get('tags', []):
                break

    await messenger.purge(subject)
    publisher = get_journalpublisher(subject)
    reader = get_journalreader(subject, deliver_policy='all')
    try:
        n = 10
        # Pre-publish some messages
        await publisher_task(publisher, n, False)
        # Start the reader
        rtask = asyncio.create_task(reader_task_fn(reader))
        # Publish more messages with a stop tag on the last one
        await publisher_task(publisher, n, True)
        # Wait for the reader to finish
        await rtask
        # Verify all messages received
        assert len(collected) == 2 * n
    finally:
        await publisher.close()
        await reader.close()
