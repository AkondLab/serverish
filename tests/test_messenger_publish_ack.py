"""Tests for publish ack-timeout handling: Nats-Msg-Id dedup header,
retries on missing ack, MessengerPublishAckTimeout."""
import asyncio

import nats.errors
import pytest

from serverish.base import MessengerPublishAckTimeout
from serverish.messenger import Messenger, get_publisher, get_reader


@pytest.mark.nats_js
async def test_publish_sets_msg_id_header(messenger, unique_subject):
    """Every publish carries Nats-Msg-Id equal to meta.id."""
    pub = get_publisher(unique_subject)
    msg = await pub.publish(data={'v': 1})
    await pub.close()

    js = Messenger().connection.js
    stream = await js.find_stream_name_by_subject(unique_subject)
    raw = await js.get_last_msg(stream, unique_subject)
    assert raw.headers is not None
    assert raw.headers.get('Nats-Msg-Id') == msg['meta']['id']


@pytest.mark.nats_js
async def test_publish_duplicate_msg_id_is_deduplicated(messenger, unique_subject):
    """Re-publishing with the same meta.id is discarded server-side
    (JetStream duplicate window) - the retry-after-ack-timeout safety net."""
    pub = get_publisher(unique_subject)
    await messenger.purge(unique_subject)
    await pub.publish(data={'v': 1}, meta={'id': 'dedup-test-fixed-id'})
    await pub.publish(data={'v': 2}, meta={'id': 'dedup-test-fixed-id'})  # duplicate
    await pub.publish(data={'v': 3})
    await pub.close()

    reader = get_reader(unique_subject, deliver_policy='all', nowait=True)
    received = [data async for data, meta in reader]
    await reader.close()

    assert received == [{'v': 1}, {'v': 3}], "duplicate id must be stored only once"


@pytest.mark.nats
async def test_publish_retries_on_ack_timeout(messenger, unique_subject):
    """A publish whose ack times out is retried and succeeds transparently."""
    pub = get_publisher(unique_subject)
    await pub.open()
    js = pub.connection.js
    original_publish = js.publish
    attempts = []

    async def flaky_publish(*args, **kwargs):
        attempts.append(kwargs.get('headers', {}).get('Nats-Msg-Id'))
        if len(attempts) == 1:
            raise nats.errors.TimeoutError
        return await original_publish(*args, **kwargs)

    js.publish = flaky_publish
    try:
        msg = await pub.publish(data={'v': 42})
    finally:
        js.publish = original_publish
        await pub.close()

    assert len(attempts) == 2, "one failed attempt + one successful retry"
    assert attempts[0] == attempts[1] == msg['meta']['id'], \
        "retry must reuse the same Nats-Msg-Id for dedup"


@pytest.mark.nats
async def test_publish_ack_timeout_raises_serverish_exception(messenger, unique_subject):
    """When retries are exhausted, a MessengerPublishAckTimeout (a TimeoutError
    subclass) is raised with an honest 'may have been delivered' message."""
    pub = get_publisher(unique_subject)
    pub.ack_timeout_retries = 1
    await pub.open()
    js = pub.connection.js
    original_publish = js.publish

    async def always_timeout(*args, **kwargs):
        raise nats.errors.TimeoutError

    js.publish = always_timeout
    try:
        with pytest.raises(MessengerPublishAckTimeout, match="MAY have been delivered"):
            await pub.publish(data={'v': 1})
        assert issubclass(MessengerPublishAckTimeout, TimeoutError)
    finally:
        js.publish = original_publish
        await pub.close()


@pytest.mark.nats
async def test_publish_ack_timeout_not_raised_when_disabled(messenger, unique_subject):
    """With raise_on_publish_error=False the ack timeout is logged and tagged,
    not raised (journal-publisher style)."""
    pub = get_publisher(unique_subject)
    pub.raise_on_publish_error = False
    pub.ack_timeout_retries = 0
    await pub.open()
    js = pub.connection.js
    original_publish = js.publish

    async def always_timeout(*args, **kwargs):
        raise nats.errors.TimeoutError

    js.publish = always_timeout
    try:
        msg = await pub.publish(data={'v': 1})
    finally:
        js.publish = original_publish
        await pub.close()

    assert 'error' in msg['meta']['tags']
