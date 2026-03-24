"""CPU-bound blocking recovery tests (COMP-02).

Tests that the serverish reader recovers after event loop starvation caused by
synchronous ``time.sleep()`` calls.  Ported from the raw nats-py test in
``test_messenger_issue10.py`` to use the serverish public API.

Two scenarios with different severity levels:

1. **Short blocking (2 s)** -- uses the regular ``messenger`` fixture whose
   default NATS ping interval (~120 s) is much longer than the block.  The
   event loop starves but the NATS server does *not* disconnect the client.
   This isolates pure starvation recovery.

2. **Long blocking (5 s)** -- uses the ``resilience_messenger`` fixture with
   ``ping_interval=2 s``.  The 5 s block exceeds the ping interval so the
   NATS server will disconnect the client.  The test documents recovery from
   both starvation *and* the subsequent automatic reconnect.
"""
from __future__ import annotations

import asyncio
import logging
import time

import pytest

from serverish.connection.connection_nats import ConnectionNATS
from serverish.messenger import Messenger, get_publisher, get_reader

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.nats, pytest.mark.timeout(60)]


def is_fixsubscriptions() -> bool:
    """Feature-detect fixsubscriptions branch via health_status property."""
    return hasattr(ConnectionNATS, 'health_status')


# ---------------------------------------------------------------------------
# Test 1 -- short blocking, pure event-loop starvation, no NATS disconnect
# ---------------------------------------------------------------------------


async def test_reader_recovers_after_short_cpu_blocking(messenger, unique_subject):
    """Reader delivers all messages after a 2 s synchronous sleep.

    The 2 s block is well under the default NATS ping_interval (~120 s) so no
    server-side disconnect should occur.  This tests pure event-loop starvation.
    """
    pub = get_publisher(subject=unique_subject)
    reader = get_reader(subject=unique_subject, deliver_policy='all')

    async with pub, reader:
        # Publish 3 messages
        for i in range(3):
            await pub.publish(data={'n': i})

        # Read the first message
        data, meta = await asyncio.wait_for(reader.read_next(), timeout=10)
        assert data['n'] == 0

        # Block the event loop -- CPU-bound work simulation (D-08)
        logger.info(
            'Blocking event loop for 2 s '
            '(under default ping_interval -- no NATS disconnect expected)'
        )
        time.sleep(2)

        # Read the remaining messages -- the reader must recover from starvation
        data, meta = await asyncio.wait_for(reader.read_next(), timeout=15)
        assert data['n'] == 1

        data, meta = await asyncio.wait_for(reader.read_next(), timeout=10)
        assert data['n'] == 2

    logger.info('Reader delivered all 3 messages after 2 s CPU blocking (pure starvation)')


# ---------------------------------------------------------------------------
# Test 2 -- long blocking, starvation + NATS disconnect
# ---------------------------------------------------------------------------


async def test_reader_recovers_after_long_cpu_blocking_with_disconnect(
    resilience_messenger, unique_subject
):
    """Reader recovers after a 5 s synchronous sleep that triggers NATS disconnect.

    With ``ping_interval=2 s`` (set by the ``resilience_messenger`` fixture), a
    5 s ``time.sleep()`` starves the event loop well beyond the NATS ping
    interval.  The NATS server will disconnect the client because outstanding
    pings are not answered.  After the sleep the client reconnects and the
    reader must deliver messages again.
    """
    m = resilience_messenger
    pub = get_publisher(subject=unique_subject)
    reader = get_reader(subject=unique_subject, deliver_policy='all')

    async with pub, reader:
        # Publish 2 messages before the block
        for i in range(2):
            await pub.publish(data={'n': i})

        # Read first message -- confirms normal operation
        data, meta = await asyncio.wait_for(reader.read_next(), timeout=10)
        assert data['n'] == 0

        # Block event loop beyond ping_interval (2 s) -- NATS disconnect expected
        logger.info(
            'Blocking event loop for 5 s -- '
            'NATS disconnect expected due to ping_interval=2 s'
        )
        time.sleep(5)

        # Give the reconnection logic time to settle
        await asyncio.sleep(3)

        # Publish a fresh message after the block + reconnect
        await pub.publish(data={'n': 99})

        # Try to read remaining messages.  The second pre-block message (n=1)
        # may or may not arrive depending on subscription state after reconnect.
        # The post-block message (n=99) MUST arrive on fixsubscriptions.
        collected = []
        try:
            for _ in range(5):  # generous upper bound
                data, meta = await asyncio.wait_for(reader.read_next(), timeout=20)
                collected.append(data['n'])
                if 99 in collected:
                    break
        except (TimeoutError, asyncio.TimeoutError):
            if not is_fixsubscriptions():
                pytest.xfail(
                    'Master may not recover from combined starvation + disconnect'
                )
            # On fixsubscriptions a timeout here is unexpected
            raise

        assert 99 in collected, (
            f'Post-block message (n=99) never arrived; collected: {collected}'
        )

        # On fixsubscriptions verify reader health via public API.
        # Note: reconnect_count tracks consumer-level recreations, not
        # transport-level reconnects.  The nats-py client may auto-reconnect
        # at the transport layer without the reader needing to recreate its
        # JetStream pull consumer, so reconnect_count may be 0 even after a
        # real disconnect.  The key proof is that messages were delivered.
        if is_fixsubscriptions():
            status = reader.health_status
            logger.info('Reader health_status after recovery: %s', status)
            assert status['is_open'], f'Reader should be open after recovery: {status}'
            assert status['messages_received'] >= 2, (
                f'Expected at least 2 messages received, got {status}'
            )

    logger.info(
        'Reader recovered after 5 s CPU blocking + NATS disconnect; '
        'collected messages: %s', collected
    )
