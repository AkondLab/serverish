"""Integration tests for policies against a live NATS (see test_policies.py for units)."""
import asyncio
import datetime
import warnings

import pytest

from serverish.messenger import (BatchPolicy, DeliverFromTime, DeliverLastPerSubject,
                                 ErrorPolicy, OnError, RetryPolicy, Until,
                                 get_publisher, get_reader)
from serverish.messenger.msg_single_pub import single_publish
from serverish.messenger.msg_single_read import single_read

UTC = datetime.timezone.utc


@pytest.mark.nats
async def test_reader_with_delivery_object(messenger, unique_subject):
    """End-to-end read driven entirely by a DeliveryPolicy object."""
    pub = get_publisher(unique_subject)
    await pub.open()
    await messenger.purge(unique_subject)
    for i in range(3):
        await pub.publish(data={'i': i})
    await asyncio.sleep(0.1)
    cutoff = datetime.datetime.now(UTC)
    for i in range(3, 6):
        await pub.publish(data={'i': i})
    await pub.close()

    reader = get_reader(unique_subject,
                        deliver_policy=DeliverFromTime(cutoff, until=Until.END_OF_DATA))
    received = [data['i'] async for data, meta in reader]
    await reader.close()
    assert received == [3, 4, 5]


@pytest.mark.nats
async def test_snapshot_with_policy_and_dynamic_batch(messenger, unique_subject):
    """tcsctl-style snapshot: last_per_subject + dynamic batching, end to end."""
    for k in range(4):
        pub = get_publisher(f"{unique_subject}.k{k}")
        await pub.open()
        for revision in range(2):
            await pub.publish(data={'k': k, 'rev': revision})
        await pub.close()
    await asyncio.sleep(0.1)

    reader = get_reader(f"{unique_subject}.>",
                        deliver_policy=DeliverLastPerSubject(until=Until.END_OF_DATA),
                        batch_policy=BatchPolicy(dynamic=True, max_batch=500))
    received = [data async for data, meta in reader]
    await reader.close()
    assert len(received) == 4
    assert all(data['rev'] == 1 for data in received)


@pytest.mark.nats
async def test_publisher_retry_policy_paces_ack_retries(messenger, unique_subject):
    """ErrorPolicy.retry drives the ack-retry loop (attempts honored)."""
    import nats.errors
    pub = get_publisher(unique_subject,
                        error_policy=ErrorPolicy(retry=RetryPolicy(attempts=2, delay=0.0)))
    await pub.open()
    js = pub.connection.js
    original_publish = js.publish
    calls = []

    async def flaky(*args, **kwargs):
        calls.append(1)
        if len(calls) <= 2:
            raise nats.errors.TimeoutError
        return await original_publish(*args, **kwargs)

    js.publish = flaky
    try:
        await pub.publish(data={'v': 1})
    finally:
        js.publish = original_publish
        await pub.close()
    assert len(calls) == 3  # 1 original + 2 retries from the policy


@pytest.mark.nats
async def test_publish_on_unopened_publisher_warns_once(messenger, unique_subject):
    pub = get_publisher(unique_subject)
    with pytest.warns(DeprecationWarning, match="never-opened"):
        await pub.publish(data={'v': 1})
    with warnings.catch_warnings():
        warnings.simplefilter("error")  # second publish must NOT warn again
        await pub.publish(data={'v': 2})
    await pub.close()


@pytest.mark.nats
async def test_publish_on_closed_publisher_warns_accurately(messenger, unique_subject):
    """A publisher that WAS opened and then closed must not be accused of
    being 'never opened' (Copilot review, PR #40)."""
    pub = get_publisher(unique_subject)
    await pub.open()
    await pub.publish(data={'v': 1})
    await pub.close()
    with pytest.warns(DeprecationWarning, match="closed publisher"):
        await pub.publish(data={'v': 2})


@pytest.mark.nats
async def test_opened_publisher_does_not_warn(messenger, unique_subject):
    async with get_publisher(unique_subject) as pub:
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            await pub.publish(data={'v': 1})


@pytest.mark.nats
async def test_messenger_close_closes_open_drivers(messenger, nats_server, unique_subject):
    """Messenger.close() is the lifecycle safety net: drivers left open are
    closed (a forgotten reader would leak a server-side consumer)."""
    from serverish.messenger import Messenger
    reader = get_reader(unique_subject, deliver_policy='last')
    await reader.open()
    try:
        await Messenger().close()
        assert reader.is_open is False
    finally:
        await Messenger().close()
        await Messenger().open(host=nats_server['host'], port=nats_server['port'])


@pytest.mark.nats
async def test_single_helpers_do_not_emit_deprecation(messenger, unique_subject):
    """single_publish/single_read are the blessed API - internal use of the
    single drivers must not trigger the factory DeprecationWarnings."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        await single_publish(unique_subject, data={'v': 7})
        data, meta = await single_read(unique_subject)
    assert data == {'v': 7}
