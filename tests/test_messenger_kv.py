"""Tests for KV bucket support: MsgKvStore, MsgKvReader, MsgKvSubscriber."""
from __future__ import annotations

import asyncio
import uuid

import pytest
import pytest_asyncio

from serverish.base import MessengerKvBucketNotFound, MessengerKvKeyNotFound, MessengerKvMalformed
from serverish.messenger import (Messenger, get_kvstore, get_kvreader, get_kvsubscriber,
                                 kv_get, kv_put)


@pytest_asyncio.fixture(loop_scope='session')
async def kv_bucket(messenger):
    """Provide a unique KV bucket name, delete the bucket on teardown."""
    bucket = f"test-kv-{uuid.uuid4().hex[:8]}"
    yield bucket
    try:
        await Messenger().connection.js.delete_key_value(bucket)
    except Exception:
        pass  # bucket may have never been created


@pytest.mark.nats_js
async def test_kv_put_get(messenger, kv_bucket):
    store = get_kvstore(kv_bucket, create_bucket=True)
    async with store:
        msg = await store.put('telescope.focus', {'position': 1250})
        assert msg['meta']['kv']['bucket'] == kv_bucket
        assert msg['meta']['kv']['key'] == 'telescope.focus'
        assert msg['meta']['kv']['revision'] >= 1

        data, meta = await store.get('telescope.focus')
        assert data == {'position': 1250}
        # full envelope round-trip: standard meta fields survive
        assert meta['id'] == msg['meta']['id']
        assert 'ts' in meta
        assert meta['kv']['operation'] == 'PUT'
        assert meta['kv']['revision'] == msg['meta']['kv']['revision']


@pytest.mark.nats_js
async def test_kv_get_missing_key(messenger, kv_bucket):
    async with get_kvstore(kv_bucket, create_bucket=True) as store:
        with pytest.raises(MessengerKvKeyNotFound):
            await store.get('no.such.key')


@pytest.mark.nats_js
async def test_kv_bucket_not_found(messenger):
    store = get_kvstore(f"test-kv-missing-{uuid.uuid4().hex[:8]}")
    with pytest.raises(MessengerKvBucketNotFound):
        await store.open()


@pytest.mark.nats_js
async def test_kv_delete(messenger, kv_bucket):
    async with get_kvstore(kv_bucket, create_bucket=True) as store:
        await store.put('ephemeral', {'v': 1})
        await store.delete('ephemeral')
        with pytest.raises(MessengerKvKeyNotFound):
            await store.get('ephemeral')


@pytest.mark.nats_js
async def test_kv_keys_and_history(messenger, kv_bucket):
    async with get_kvstore(kv_bucket, create_bucket=True, bucket_config={'history': 5}) as store:
        assert await store.keys() == []

        await store.put('alpha', {'v': 1})
        await store.put('alpha', {'v': 2})
        await store.put('beta', {'v': 3})

        assert sorted(await store.keys()) == ['alpha', 'beta']

        history = await store.history('alpha')
        assert [data for data, meta in history] == [{'v': 1}, {'v': 2}]
        revisions = [meta['kv']['revision'] for data, meta in history]
        assert revisions == sorted(revisions)


@pytest.mark.nats_js
async def test_kv_malformed_entry(messenger, kv_bucket):
    async with get_kvstore(kv_bucket, create_bucket=True) as store:
        # bypass serverish and write a raw value, as a foreign client would
        await store.kv.put('foreign', b'not a serverish envelope')
        with pytest.raises(MessengerKvMalformed):
            await store.get('foreign')


@pytest.mark.nats_js
async def test_kv_oneshot(messenger, kv_bucket):
    await kv_put(kv_bucket, 'dome.status', {'open': True}, create_bucket=True)
    data, meta = await kv_get(kv_bucket, 'dome.status')
    assert data == {'open': True}
    assert meta['kv']['key'] == 'dome.status'


@pytest.mark.nats_js
async def test_kv_reader_watch(messenger, kv_bucket):
    async with get_kvstore(kv_bucket, create_bucket=True) as store:
        await store.put('alpha', {'v': 1})

        reader = get_kvreader(kv_bucket)
        await reader.open()
        try:
            got = []

            async def consume():
                async for data, meta in reader:
                    got.append((data, meta))
                    if len(got) >= 3:
                        break

            consumer = asyncio.create_task(consume())
            await asyncio.sleep(0.3)  # let the watcher deliver the initial value
            await store.put('beta', {'v': 2})
            await store.delete('alpha')
            await asyncio.wait_for(consumer, timeout=5)
        finally:
            await reader.close()

        # initial replay of the existing key, then live updates in order
        assert got[0][0] == {'v': 1}
        assert got[0][1]['kv']['key'] == 'alpha'
        assert got[1][0] == {'v': 2}
        assert got[1][1]['kv']['key'] == 'beta'
        # delete marker carries no envelope
        assert got[2][0] is None
        assert got[2][1]['kv'] == {**got[2][1]['kv'], 'key': 'alpha', 'operation': 'DEL'}


@pytest.mark.nats_js
async def test_kv_subscriber_callback(messenger, kv_bucket):
    async with get_kvstore(kv_bucket, create_bucket=True) as store:
        events = []
        done = asyncio.Event()

        def callback(data, meta):
            events.append((data, meta))
            if len(events) >= 2:
                done.set()
                return False

        sub = get_kvsubscriber(kv_bucket)
        await sub.open()
        try:
            await sub.subscribe(callback)
            await store.put('k1', {'n': 1})
            await store.put('k2', {'n': 2})
            await asyncio.wait_for(done.wait(), timeout=5)
        finally:
            await sub.close()

        assert [data for data, meta in events] == [{'n': 1}, {'n': 2}]
        assert [meta['kv']['key'] for data, meta in events] == ['k1', 'k2']
