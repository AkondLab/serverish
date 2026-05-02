import logging
import asyncio
import datetime

import pytest

from serverish.messenger import Messenger, get_publisher, get_reader, get_callbacksubscriber


@pytest.mark.nats
async def test_messenger_pub_sub_cb(messenger, unique_subject):

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S.%f')
    now = datetime.datetime.now()

    msgs = []
    def cb(data, meta):
        print(data)
        msgs.append(data)
        return True

    async def publisher_task(pub, n):
        for i in range(n):
            await pub.publish(data={'n': i, 'final': False})
            await asyncio.sleep(0.1)

    pub = get_publisher(subject=unique_subject)
    await messenger.purge(subject=unique_subject)
    await publisher_task(pub, 4)
    await asyncio.sleep(0.1)
    sub = get_callbacksubscriber(subject=unique_subject, deliver_policy='all')
    async with sub:
        await sub.subscribe(cb)
        await sub.wait_for_empty()
        assert len(msgs) == 4
        await asyncio.sleep(1)
        await publisher_task(pub, 5)
        await asyncio.sleep(6)
        await sub.wait_for_empty()
        assert len(msgs) == 9


async def _wait_until(predicate, timeout: float = 5.0, interval: float = 0.1) -> bool:
    """Poll predicate up to timeout seconds; return True as soon as it holds."""
    for _ in range(int(timeout / interval)):
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()


@pytest.mark.nats
async def test_async_callback_returning_none_keeps_subscription_alive(messenger, unique_subject):
    """Regression: async callback with no explicit return (returns None) must
    not stop the subscription loop.

    The buggy predicate `if not cont` fires for both False and None — so any
    `async def cb(...)` that does not explicitly `return True` silently broke
    the subscription after the first delivered message. Production symptom:
    pms aggregator's 144 cache.put callbacks died after one replay message.
    """
    received = []

    async def cb(data, meta):
        received.append(data['n'])
        # implicit return None — the idiomatic case the bug breaks

    pub = get_publisher(subject=unique_subject)
    await messenger.purge(subject=unique_subject)

    sub = get_callbacksubscriber(subject=unique_subject, deliver_policy='all')
    async with sub:
        await sub.subscribe(cb)
        for i in range(5):
            await pub.publish(data={'n': i})
            await asyncio.sleep(0.05)
        await _wait_until(lambda: len(received) >= 5, timeout=5.0)
        assert received == [0, 1, 2, 3, 4], (
            f"Async callback returning None must not stop the loop. "
            f"Received {len(received)}/5: {received}"
        )


@pytest.mark.nats
async def test_async_callback_returning_false_stops_subscription(messenger, unique_subject):
    """The documented stop-on-False contract must still hold after the fix."""
    received = []

    async def cb(data, meta):
        received.append(data['n'])
        return False  # explicit stop after first message

    pub = get_publisher(subject=unique_subject)
    await messenger.purge(subject=unique_subject)

    sub = get_callbacksubscriber(subject=unique_subject, deliver_policy='all')
    async with sub:
        await sub.subscribe(cb)
        for i in range(5):
            await pub.publish(data={'n': i})
            await asyncio.sleep(0.05)
        # Give the loop ample time to (incorrectly) keep going if broken
        await asyncio.sleep(1.5)
        assert received == [0], (
            f"Returning False must stop the loop after one message. Received {received}"
        )


@pytest.mark.nats
async def test_sync_callback_returning_none_keeps_subscription_alive(messenger, unique_subject):
    """Same regression as the async case, for sync callbacks — `def cb` with
    no explicit return also returns None and must keep the loop alive."""
    received = []

    def cb(data, meta):
        received.append(data['n'])
        # implicit return None

    pub = get_publisher(subject=unique_subject)
    await messenger.purge(subject=unique_subject)

    sub = get_callbacksubscriber(subject=unique_subject, deliver_policy='all')
    async with sub:
        await sub.subscribe(cb)
        for i in range(5):
            await pub.publish(data={'n': i})
            await asyncio.sleep(0.05)
        await _wait_until(lambda: len(received) >= 5, timeout=5.0)
        assert received == [0, 1, 2, 3, 4], (
            f"Sync callback returning None must not stop the loop. "
            f"Received {len(received)}/5: {received}"
        )
