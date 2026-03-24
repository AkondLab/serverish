"""Slow consumer detection test (COMP-04).

Proves that ``slow_consumer_count`` on the NATS connection increments when a
push subscription is overwhelmed by a rapid message burst.  Uses the
``health_status`` public API as the primary assertion mechanism, falling back
to the private ``_slow_consumer_count`` attribute only when the public API
is unavailable (master branch).

The trigger mechanism uses a raw NATS core push subscription with very low
``pending_msgs_limit`` and ``pending_bytes_limit`` so that even a moderate
burst of messages overflows the buffer and triggers ``SlowConsumerError``
events on the shared nats-py client.  Because the Messenger's
``ConnectionNATS`` registers an ``error_cb`` on that same client, the slow
consumer counter is incremented for any subscription on the connection.
"""
from __future__ import annotations

import asyncio
import logging

import pytest

from serverish.connection.connection_nats import ConnectionNATS
from serverish.messenger import Messenger

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.nats, pytest.mark.timeout(30)]


def is_fixsubscriptions() -> bool:
    """Feature-detect fixsubscriptions branch via health_status property."""
    return hasattr(ConnectionNATS, 'health_status')


async def test_slow_consumer_count_increments_under_burst(messenger, nats_server, unique_subject):
    """Slow consumer events are tracked when a push subscription is overwhelmed.

    A raw NATS push subscription with very low pending limits is flooded with
    messages.  The ``SlowConsumerError`` events fire on the shared nats-py
    client and are counted by ``ConnectionNATS.nats_error_cb``.
    """
    m = messenger  # Use the fixture-provided Messenger (not Messenger() singleton)

    # If a previous test module's fixture (e.g. resilience_messenger) closed the
    # Messenger singleton, reopen it so this test can proceed.
    if m.conn is None:
        logger.info('Messenger connection was closed by a prior fixture, reopening')
        await m.open(host=nats_server['host'], port=nats_server['port'])

    nc = m.connection.nc

    # Read initial slow consumer count via public API (preferred) or private attr
    if is_fixsubscriptions():
        initial_count = m.connection.health_status['slow_consumer_count']
    else:
        if not hasattr(m.connection, '_slow_consumer_count'):
            pytest.skip('slow_consumer_count tracking not available on this branch')
        initial_count = m.connection._slow_consumer_count

    # Deliberately slow callback to simulate a consumer that cannot keep up
    received = []

    async def slow_callback(msg):
        await asyncio.sleep(0.1)
        received.append(msg)

    # Subscribe with very low pending limits to trigger slow consumer errors
    sub = await nc.subscribe(
        unique_subject,
        cb=slow_callback,
        pending_msgs_limit=5,
        pending_bytes_limit=1024,
    )

    # Flood the subscription with messages using the raw NATS connection
    padding = 'x' * 200
    for i in range(100):
        await nc.publish(
            unique_subject,
            f'{{"burst": {i}, "padding": "{padding}"}}'.encode(),
        )
    await nc.flush()

    # Wait for slow consumer errors to propagate through the error callback
    await asyncio.sleep(2)

    # Assert that slow consumer count increased
    if is_fixsubscriptions():
        final_count = m.connection.health_status['slow_consumer_count']
    else:
        final_count = m.connection._slow_consumer_count

    assert final_count > initial_count, (
        f'Expected slow_consumer_count to increase from {initial_count}, '
        f'got {final_count}'
    )

    logger.info(
        'Slow consumer events detected: %d (from %d to %d)',
        final_count - initial_count, initial_count, final_count,
    )

    # Cleanup
    await sub.unsubscribe()
