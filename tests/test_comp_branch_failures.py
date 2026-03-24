"""Comparative tests: scenarios that pass on fixsubscriptions but fail on master (COMP-01).

Proves that fixsubscriptions branch solves reliability problems that master cannot handle:
1. Silent consumer expiry -- master's ensure_consumer only checks after error, not proactively
2. Reconnection during fetch -- master's monolithic 100s blocking fetch with no reconnection checks

Branch detection uses feature detection (hasattr on ConnectionNATS.health_status),
not git commands or version parsing, for deterministic behavior in all CI environments.
"""
from __future__ import annotations

import asyncio
import logging
import time

import pytest

from serverish.connection.connection_nats import ConnectionNATS
from serverish.messenger import Messenger, get_publisher, get_reader

logger = logging.getLogger(__name__)


def is_fixsubscriptions() -> bool:
    """Detect fixsubscriptions branch by checking for architectural features.

    Feature detection is deterministic regardless of CI environment
    (detached HEAD, shallow clones, etc). The health_status property
    exists ONLY on fixsubscriptions.
    """
    return hasattr(ConnectionNATS, 'health_status')


is_master = not is_fixsubscriptions()

pytestmark = [
    pytest.mark.nats,
    pytest.mark.nats_resilience,
    pytest.mark.timeout(120),
]


async def _force_disconnect_detection(messenger: Messenger, timeout: float = 15.0) -> None:
    """Force the NATS client to detect a broken connection.

    A paused container freezes TCP -- the client cannot detect this passively.
    We attempt a flush with a short timeout to trigger detection, then poll
    nc.is_connected as the nats-py client processes the disconnect asynchronously.

    Uses nc.is_connected directly (not health_status) for master compatibility.
    """
    start = time.monotonic()
    # Phase 1: trigger detection via failed flush
    while time.monotonic() - start < timeout:
        try:
            await asyncio.wait_for(
                messenger.connection.nc.flush(),
                timeout=2.0,
            )
            await asyncio.sleep(0.5)
        except Exception:
            logger.info('Flush failed -- disconnect triggered')
            break
    # Phase 2: wait for nats-py to process the disconnect internally
    while time.monotonic() - start < timeout:
        if not messenger.connection.nc.is_connected:
            logger.info('Connection now reports disconnected')
            return
        await asyncio.sleep(0.3)
    logger.warning(
        'Could not confirm is_connected=False within timeout; proceeding'
    )


async def _poll_until_connected(messenger: Messenger, timeout: float = 20.0) -> None:
    """Poll until the NATS connection reports as connected again.

    Uses nc.is_connected directly (not health_status) for master compatibility.
    """
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if messenger.connection.nc.is_connected:
            return
        await asyncio.sleep(0.3)
    raise TimeoutError('Connection did not reconnect within timeout')


@pytest.mark.xfail(
    condition=is_master,
    reason='Master lacks proactive consumer health checks -- ensure_consumer only checks after error',
    strict=True,
)
async def test_reader_recovers_from_silent_consumer_expiry(
    resilience_messenger, nats_disruptor, unique_subject,
):
    """Silent consumer expiry: the 'smoking gun' proving fixsubscriptions superiority.

    On master, when an ephemeral consumer expires during a network partition,
    the reader never detects this because ensure_consumer() only checks when
    self.error is not None. The consumer silently disappears and messages
    stop flowing with no error and no recovery.

    On fixsubscriptions, proactive consumer health checks detect the expired
    consumer and automatically recreate it.
    """
    m = resilience_messenger
    pub = get_publisher(subject=unique_subject)
    reader = get_reader(
        subject=unique_subject,
        deliver_policy='all',
        inactive_threshold=5,
    )
    try:
        # --- 1. BASELINE ---
        await pub.publish(data={'phase': 'baseline', 'n': 1})
        data, meta = await asyncio.wait_for(reader.read_next(), timeout=10)
        assert data['phase'] == 'baseline'
        assert data['n'] == 1
        logger.info('BASELINE passed: message received normally')

        # --- 2. DISRUPT ---
        logger.info('DISRUPTING: pausing NATS container for 8s (inactive_threshold=5s)')
        nats_disruptor.pause()
        await asyncio.sleep(8)

        # --- 3. VERIFY DEGRADED ---
        await _force_disconnect_detection(m, timeout=10)
        if not m.connection.nc.is_connected:
            logger.info('DEGRADED verified: connection reports disconnected')
        else:
            logger.warning('Connection still reports connected during pause')

        # --- 4. RESTORE ---
        logger.info('RESTORING: unpausing NATS container')
        nats_disruptor.unpause()
        await _poll_until_connected(m, timeout=20)

        # --- 5. VERIFY RECOVERY ---
        # This is where master fails: the reader's consumer expired but
        # master's ensure_consumer only checks when self.error is not None.
        await pub.publish(data={'phase': 'after_expiry', 'n': 2})
        data, meta = await asyncio.wait_for(reader.read_next(), timeout=30)
        assert data['phase'] == 'after_expiry'
        logger.info('RECOVERY verified: message delivered after consumer expiry')

        # --- 6. VERIFY METRICS (fixsubscriptions only) ---
        if is_fixsubscriptions():
            reader_status = reader.health_status
            assert reader_status['reconnect_count'] >= 1, (
                f'Expected reconnect_count >= 1, got: {reader_status["reconnect_count"]}'
            )
            logger.info('METRICS verified: reconnect_count=%d', reader_status['reconnect_count'])

    finally:
        try:
            nats_disruptor.unpause()
        except Exception:
            pass
        await pub.close()
        await reader.close()


@pytest.mark.xfail(
    condition=is_master,
    reason='Master blocks in single 100s fetch with no reconnection checks',
    strict=True,
)
async def test_reader_recovers_from_reconnect_during_fetch(
    resilience_messenger, nats_disruptor, unique_subject,
):
    """Reconnection during active fetch: master's monolithic 100s blocking wait fails.

    On master, read_next issues a single fetch with a 100s timeout. If a
    disconnect occurs mid-fetch, the inbox subscription becomes stale but
    the fetch call blocks until the full timeout expires. No reconnection
    checks happen during the wait.

    On fixsubscriptions, fetches are segmented into shorter intervals with
    reconnection checks between segments, allowing the reader to detect
    disconnection and re-establish the consumer.
    """
    m = resilience_messenger
    pub = get_publisher(subject=unique_subject)
    reader = get_reader(subject=unique_subject, deliver_policy='all')
    try:
        # --- 1. BASELINE ---
        await pub.publish(data={'phase': 'baseline', 'n': 1})
        data, meta = await asyncio.wait_for(reader.read_next(), timeout=10)
        assert data['phase'] == 'baseline'
        logger.info('BASELINE passed: message received normally')

        # --- 2. START BLOCKING READ ---
        read_task = asyncio.create_task(reader.read_next())
        await asyncio.sleep(1)  # Let reader enter blocking fetch

        # --- 3. DISRUPT ---
        logger.info('DISRUPTING: pausing NATS container during active fetch')
        nats_disruptor.pause()
        await asyncio.sleep(3)
        await _force_disconnect_detection(m, timeout=10)

        logger.info('RESTORING: unpausing NATS container')
        nats_disruptor.unpause()
        await _poll_until_connected(m, timeout=20)

        # --- 4. PUBLISH AFTER RECONNECT ---
        published = False
        start = time.monotonic()
        for attempt in range(20):
            try:
                await pub.publish(data={'phase': 'after_reconnect', 'n': 2})
                published = True
                logger.info('Published after reconnect (attempt %d)', attempt + 1)
                break
            except Exception as e:
                logger.debug('Publish attempt %d failed: %s', attempt + 1, e)
                await asyncio.sleep(0.5)
        assert published, 'Could not publish after reconnect within retry limit'

        # --- 5. VERIFY RECOVERY ---
        # Master fails here: stuck in stale fetch() with dead inbox.
        data, meta = await asyncio.wait_for(read_task, timeout=30)
        assert data['phase'] == 'after_reconnect'
        logger.info('RECOVERY verified: reader received message after mid-fetch disruption')

        # --- 6. VERIFY METRICS (fixsubscriptions only) ---
        if is_fixsubscriptions():
            reader_status = reader.health_status
            # The reader may recover via the fetch loop without a full reconnect
            # cycle (reconnect_count stays 0). The definitive proof is step 5:
            # the message was received after disruption.
            logger.info(
                'METRICS: reconnect_count=%d, messages_received=%d, is_open=%s',
                reader_status['reconnect_count'],
                reader_status['messages_received'],
                reader_status['is_open'],
            )
            assert reader_status['messages_received'] >= 2

    finally:
        try:
            nats_disruptor.unpause()
        except Exception:
            pass
        if not read_task.done():
            read_task.cancel()
            try:
                await read_task
            except (asyncio.CancelledError, Exception):
                pass
        await pub.close()
        await reader.close()
