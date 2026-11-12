"""Subjects, not streams (issue #49): the persistence contract of publishers,
subject-family readers and the KV reader which finishes.

None of the public API exercised here mentions streams, consumers, filters
or sequence numbers.
"""
from __future__ import annotations

import asyncio
import uuid
from unittest import mock

import pytest

from serverish.base import MessengerNotPersisted, ServerishError
from serverish.messenger import (Messenger, get_publisher, get_reader, get_corepublisher, get_corereader,
                                 get_callbacksubscriber, get_kvstore, get_kvreader)
from serverish.messenger import msg_publisher
from serverish.messenger.messenger import subject_family


@pytest.fixture
def volatile_subject(request):
    """A subject no stream covers (the test stream takes 'test.>')"""
    return f'test_no_js.{request.node.name}.{uuid.uuid4().hex[:8]}'


# ---------------------------------------------------------------------------
# Persistence contract
# ---------------------------------------------------------------------------

@pytest.mark.nats_js
async def test_is_persistent(messenger, unique_subject, volatile_subject):
    assert await Messenger().is_persistent(unique_subject) is True
    assert await Messenger().is_persistent(volatile_subject) is False


@pytest.mark.nats_js
async def test_persistent_publisher_on_persistent_subject(messenger, unique_subject):
    async with get_publisher(unique_subject, persistent=True) as pub:
        msg = await pub.publish(data={'v': 1})
    assert 'error' not in msg['meta']['tags']
    reader = get_reader(unique_subject, deliver_policy='all', nowait=True)
    assert [data async for data, _ in reader] == [{'v': 1}]
    await reader.close()


@pytest.mark.nats_js
async def test_persistent_publisher_refuses_to_open_on_volatile_subject(messenger, volatile_subject):
    """A misconfigured service fails at startup, not on its first message"""
    pub = get_publisher(volatile_subject, persistent=True)
    with pytest.raises(MessengerNotPersisted) as ei:
        await pub.open()
    assert not pub.is_open
    assert ei.value.subject == volatile_subject
    assert volatile_subject in str(ei.value)
    assert isinstance(ei.value, ServerishError)


@pytest.mark.nats_js
async def test_persistent_publish_fails_when_message_is_not_stored(messenger, volatile_subject):
    """Also without open() (or if persistence is lost later): published must mean stored"""
    pub = get_publisher(volatile_subject, persistent=True)
    with pytest.raises(MessengerNotPersisted):
        await pub.publish(data={'v': 1})
    assert pub.health_status['error_count'] == 1
    assert pub.health_status['publish_count'] == 0

    pub.raise_on_publish_error = False
    msg = await pub.publish(data={'v': 1})
    assert 'error' in msg['meta']['tags']
    assert 'not persistent' in msg['meta']['status']


@pytest.mark.nats_js
async def test_default_publisher_warns_once_about_volatile_subject(messenger, volatile_subject):
    """2.x default: the legacy leniency stays, but it is no longer silent"""
    pub = get_publisher(volatile_subject)
    with mock.patch.object(msg_publisher.log, 'warning') as warning:
        async with pub:
            for _ in range(3):
                msg = await pub.publish(data={'v': 1})
                assert 'error' not in msg['meta']['tags']
    assert warning.call_count == 1, "one warning per publisher, not per message"
    assert volatile_subject in warning.call_args.args[0]
    assert 'not persistent' in warning.call_args.args[0]
    assert pub.health_status['publish_count'] == 3


@pytest.mark.nats_js
async def test_default_publisher_is_quiet_on_persistent_subject(messenger, unique_subject):
    with mock.patch.object(msg_publisher.log, 'warning') as warning:
        async with get_publisher(unique_subject) as pub:
            await pub.publish(data={'v': 1})
    warning.assert_not_called()


# ---------------------------------------------------------------------------
# Subject family
# ---------------------------------------------------------------------------

def test_subject_family_patterns():
    assert subject_family('a.b.c') == ['a.b.c', 'a.b.c.>']
    assert subject_family('a.*.c') == ['a.*.c', 'a.*.c.>']
    with pytest.raises(ValueError):
        subject_family('a.b.>')


def test_family_of_everything_below_is_rejected_at_construction():
    with pytest.raises(ValueError):
        get_reader('test.x.>', family=True)
    with pytest.raises(ValueError):
        get_corereader('test_no_js.x.>', family=True)


async def _publish(subject: str, data: dict, core: bool = False) -> None:
    pub = get_corepublisher(subject) if core else get_publisher(subject, persistent=True)
    async with pub:
        await pub.publish(data=data)


@pytest.mark.nats_js
async def test_family_reader_takes_subject_and_suffixes_but_not_siblings(messenger, unique_subject):
    base = f'{unique_subject}.item'
    await _publish(base, {'n': 0})
    await _publish(f'{base}.key1', {'n': 1})
    await _publish(f'{base}.key2.deeper', {'n': 2})
    await _publish(f'{unique_subject}.itemx', {'n': 'sibling'})
    await _publish(f'{unique_subject}.other', {'n': 'sibling'})

    reader = get_reader(base, deliver_policy='all', nowait=True, family=True)
    received = [(meta['nats']['subject'], data['n']) async for data, meta in reader]
    await reader.close()
    assert received == [(base, 0), (f'{base}.key1', 1), (f'{base}.key2.deeper', 2)]

    # without family: the bare subject only, as always
    reader = get_reader(base, deliver_policy='all', nowait=True)
    assert [data['n'] async for data, _ in reader] == [0]
    await reader.close()


@pytest.mark.nats_js
async def test_family_with_last_per_subject_is_a_keyed_collection(messenger, unique_subject):
    """Latest message per key - the persistent-subject twin of a KV bucket"""
    base = f'{unique_subject}.processed'
    await _publish(f'{base}.file_a', {'state': 'started'})
    await _publish(f'{base}.file_b', {'state': 'started'})
    await _publish(f'{base}.file_a', {'state': 'done'})

    reader = get_reader(base, deliver_policy='last_per_subject', nowait=True, family=True)
    latest = {meta['nats']['subject'].rsplit('.', 1)[-1]: data['state'] async for data, meta in reader}
    await reader.close()
    assert latest == {'file_a': 'done', 'file_b': 'started'}


@pytest.mark.nats_js
async def test_family_callback_subscriber(messenger, unique_subject):
    base = f'{unique_subject}.item'
    received = []
    done = asyncio.Event()

    async def on_message(data, meta):
        received.append(meta['nats']['subject'])
        if len(received) == 2:
            done.set()

    sub = get_callbacksubscriber(base, deliver_policy='new', family=True)
    await sub.open()
    try:
        await sub.subscribe(on_message)
        await asyncio.sleep(0.3)
        await _publish(f'{unique_subject}.sibling', {})
        await _publish(base, {})
        await _publish(f'{base}.key', {})
        await asyncio.wait_for(done.wait(), timeout=8)
    finally:
        await sub.close()
    assert received == [base, f'{base}.key']


@pytest.mark.nats
async def test_family_core_reader(messenger, volatile_subject):
    base = f'{volatile_subject}.item'
    reader = get_corereader(base, family=True)
    async with reader:
        await _publish(f'{volatile_subject}.itemx', {'n': 'sibling'}, core=True)
        await _publish(base, {'n': 0}, core=True)
        await _publish(f'{base}.key', {'n': 1}, core=True)
        received = []
        for _ in range(2):
            data, meta = await asyncio.wait_for(reader.read_next(), timeout=5)
            received.append((meta['nats']['subject'], data['n']))
    assert sorted(received) == [(base, 0), (f'{base}.key', 1)]


# ---------------------------------------------------------------------------
# KV reader which finishes
# ---------------------------------------------------------------------------

@pytest.mark.nats_js
async def test_kv_reader_nowait_delivers_current_state_and_finishes(messenger):
    bucket = f"test-kv-{uuid.uuid4().hex[:8]}"
    try:
        async with get_kvstore(bucket, create_bucket=True) as store:
            # empty bucket: nothing to deliver, the iteration just ends
            async with get_kvreader(bucket, nowait=True) as reader:
                assert await asyncio.wait_for(_collect(reader), timeout=5) == {}

            await store.put('file_a', {'state': 'started'})
            await store.put('file_b', {'state': 'done'})
            await store.put('file_a', {'state': 'done'})

            async with get_kvreader(bucket, nowait=True) as reader:
                state = await asyncio.wait_for(_collect(reader), timeout=5)
            assert state == {'file_a': {'state': 'done'}, 'file_b': {'state': 'done'}}

            async with get_kvreader(bucket, key='file_b', nowait=True) as reader:
                assert await asyncio.wait_for(_collect(reader), timeout=5) == {'file_b': {'state': 'done'}}
    finally:
        await Messenger().connection.js.delete_key_value(bucket)


async def _collect(reader) -> dict:
    return {meta['kv']['key']: data async for data, meta in reader}


# ---------------------------------------------------------------------------
# Review follow-ups (PR #50)
# ---------------------------------------------------------------------------

@pytest.mark.nats_js
async def test_family_reader_when_only_the_keyed_subjects_are_persistent(messenger):
    """A keyed collection stored as 'a.b.>' only: the bare 'a.b' is not persistent,
    the family reader must still find and read the keys"""
    from nats.js.api import StreamConfig
    js = Messenger().connection.js
    name = f"famtest{uuid.uuid4().hex[:8]}"
    base = f'{name}.item'
    await js.add_stream(StreamConfig(name=name, subjects=[f'{base}.>'], storage='memory'))
    try:
        assert await Messenger().is_persistent(base) is False
        await _publish(f'{base}.key1', {'n': 1})
        await _publish(f'{base}.key2', {'n': 2})
        reader = get_reader(base, deliver_policy='all', nowait=True, family=True)
        received = [(meta['nats']['subject'], data['n']) async for data, meta in reader]
        await reader.close()
        assert received == [(f'{base}.key1', 1), (f'{base}.key2', 2)]
    finally:
        await js.delete_stream(name)


@pytest.mark.nats_js
async def test_family_reader_refuses_a_server_ignoring_its_filters(messenger, unique_subject):
    """NATS < 2.10 ignores multiple filters and would deliver the whole storage:
    the reader must notice, remove the consumer and fail clearly"""
    from serverish.base import MessengerReaderConfigError
    js = Messenger().connection.js
    original = js.pull_subscribe
    created = []

    async def old_server_pull_subscribe(*args, **kwargs):
        subscription = await original(*args, **kwargs)
        info = await subscription.consumer_info()
        created.append((info.stream_name, info.name))
        info.config.filter_subjects = None  # what a server unaware of the field reports back
        info.config.filter_subject = None
        subscription.consumer_info = mock.AsyncMock(return_value=info)
        return subscription

    reader = get_reader(f'{unique_subject}.item', deliver_policy='all', nowait=True, family=True)
    with mock.patch.object(js, 'pull_subscribe', old_server_pull_subscribe):
        with pytest.raises(MessengerReaderConfigError, match='2.10'):
            await reader.open()
        assert not reader.is_open
        # the read loop treats it as fatal too - no endless retrying
        with pytest.raises(MessengerReaderConfigError, match='2.10'):
            await asyncio.wait_for(reader.read_next(), timeout=5)

    import nats.js.errors
    for stream, consumer in created:
        with pytest.raises(nats.js.errors.NotFoundError):
            await js.consumer_info(stream, consumer)


@pytest.mark.nats
async def test_family_core_reader_subscribes_all_or_nothing(messenger, volatile_subject):
    from serverish.base import MessengerConnectionClosed
    import nats.errors
    nc = Messenger().connection.nc
    original = nc.subscribe
    made = []

    async def second_fails(subject, **kwargs):
        if made:
            raise nats.errors.ConnectionClosedError()
        made.append(await original(subject, **kwargs))
        return made[-1]

    reader = get_corereader(f'{volatile_subject}.item', family=True)
    with mock.patch.object(nc, 'subscribe', second_fails):
        with pytest.raises(MessengerConnectionClosed):
            await reader.open()
    assert reader.subscriptions == []
    assert not reader.is_open
    assert len(made) == 1 and made[0]._closed, "the first subscription must be rolled back"


@pytest.mark.nats_js
async def test_kv_reader_nowait_stays_finished(messenger):
    bucket = f"test-kv-{uuid.uuid4().hex[:8]}"
    try:
        async with get_kvstore(bucket, create_bucket=True) as store:
            await store.put('a', {'v': 1})
            async with get_kvreader(bucket, nowait=True) as reader:
                assert await asyncio.wait_for(_collect(reader), timeout=5) == {'a': {'v': 1}}
                await store.put('b', {'v': 2})  # a later change must not revive the finished reader
                assert await asyncio.wait_for(_collect(reader), timeout=5) == {}
            # reopening takes a fresh snapshot
            async with get_kvreader(bucket, nowait=True) as reader:
                assert await asyncio.wait_for(_collect(reader), timeout=5) == {'a': {'v': 1}, 'b': {'v': 2}}
    finally:
        await Messenger().connection.js.delete_key_value(bucket)


@pytest.mark.nats_js
async def test_family_split_between_storages_is_rejected_not_read_in_part(messenger):
    """A family whose bare subject and keyed subjects live in different storages cannot
    be read by one reader - half of it presented as the whole would be a silent lie"""
    from nats.js.api import StreamConfig
    from serverish.base import MessengerReaderConfigError
    js = Messenger().connection.js
    name = f"famsplit{uuid.uuid4().hex[:8]}"
    base = f'{name}.item'
    await js.add_stream(StreamConfig(name=f'{name}_bare', subjects=[base], storage='memory'))
    await js.add_stream(StreamConfig(name=f'{name}_keys', subjects=[f'{base}.>'], storage='memory'))
    try:
        await _publish(base, {'n': 0})
        await _publish(f'{base}.key', {'n': 1})
        reader = get_reader(base, deliver_policy='all', nowait=True, family=True)
        with pytest.raises(MessengerReaderConfigError, match="several places"):
            await reader.open()
        assert not reader.is_open
        # each part is still readable on its own
        for subject, expected in ((base, [0]), (f'{base}.>', [1])):
            part = get_reader(subject, deliver_policy='all', nowait=True)
            assert [data['n'] async for data, _ in part] == expected
            await part.close()
    finally:
        await js.delete_stream(f'{name}_bare')
        await js.delete_stream(f'{name}_keys')


@pytest.mark.nats
async def test_family_core_reader_close_can_be_retried(messenger, volatile_subject):
    """A subscription which could not be removed is kept, so a repeated close() finishes the cleanup"""
    import nats.errors
    from serverish.base import MessengerConnectionClosed
    reader = get_corereader(f'{volatile_subject}.item', family=True)
    await reader.open()
    first, second = reader.subscriptions
    with mock.patch.object(first, 'unsubscribe', mock.AsyncMock(side_effect=nats.errors.ConnectionClosedError())):
        with pytest.raises(MessengerConnectionClosed):
            await reader.close()
    assert reader.subscriptions == [first], "the failed one is kept, the other one is gone"
    assert second._closed
    await reader.close()
    assert reader.subscriptions == [] and first._closed
    assert not reader.is_open


@pytest.mark.nats_js
async def test_family_with_keys_split_between_storages_is_rejected(messenger):
    """Keyed subjects stored in two places ('a.b.east.>' here, 'a.b.west.>' there), the bare
    subject nowhere: one lookup would report a single storage and silently omit the other half"""
    from nats.js.api import StreamConfig
    from serverish.base import MessengerReaderConfigError
    js = Messenger().connection.js
    name = f"famkeys{uuid.uuid4().hex[:8]}"
    base = f'{name}.item'
    await js.add_stream(StreamConfig(name=f'{name}_east', subjects=[f'{base}.east.>'], storage='memory'))
    await js.add_stream(StreamConfig(name=f'{name}_west', subjects=[f'{base}.west.>'], storage='memory'))
    try:
        assert sorted(await Messenger().connection.subject_storages(f'{base}.>')) == [f'{name}_east', f'{name}_west']
        await _publish(f'{base}.east.k', {'n': 1})
        await _publish(f'{base}.west.k', {'n': 2})
        reader = get_reader(base, deliver_policy='all', nowait=True, family=True)
        with pytest.raises(MessengerReaderConfigError, match='several places'):
            await reader.open()
        # a family within one of the storages reads fine
        reader = get_reader(f'{base}.east', deliver_policy='all', nowait=True, family=True)
        assert [data['n'] async for data, _ in reader] == [1]
        await reader.close()
    finally:
        await js.delete_stream(f'{name}_east')
        await js.delete_stream(f'{name}_west')


@pytest.mark.nats
async def test_family_core_reader_keeps_a_subscription_it_could_not_roll_back(messenger, volatile_subject):
    """Subscribe fails half-way and the rollback fails too: the live subscription stays known,
    so close() can still remove it"""
    import nats.errors
    from serverish.base import MessengerConnectionClosed
    nc = Messenger().connection.nc
    original = nc.subscribe
    made = []

    async def second_fails(subject, **kwargs):
        if made:
            raise nats.errors.ConnectionClosedError()
        subscription = await original(subject, **kwargs)
        real_unsubscribe = subscription.unsubscribe
        attempts = []

        async def unsubscribe_fails_once(*args, **kw):
            attempts.append(1)
            if len(attempts) == 1:
                raise nats.errors.ConnectionClosedError()
            return await real_unsubscribe(*args, **kw)

        subscription.unsubscribe = unsubscribe_fails_once
        made.append(subscription)
        return subscription

    reader = get_corereader(f'{volatile_subject}.item', family=True)
    with mock.patch.object(nc, 'subscribe', second_fails):
        with pytest.raises(MessengerConnectionClosed):
            await reader.open()
    assert reader.subscriptions == made, "the subscription which is still live must not be forgotten"
    await reader.close()
    assert reader.subscriptions == [] and made[0]._closed
