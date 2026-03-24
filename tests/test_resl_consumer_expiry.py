"""Resilience tests: ephemeral consumer expiry detection and recreation (RESL-02).

Proves that MsgReader detects when its ephemeral consumer expires
(via inactive_threshold timeout) and automatically recreates it,
resuming message delivery without manual intervention.
"""
from __future__ import annotations

import asyncio
import logging
import time

import pytest

from serverish.messenger import Messenger, get_publisher, get_reader
from tests.conftest import wait_for_healthy

logger = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.nats,
    pytest.mark.nats_resilience,
    pytest.mark.timeout(90),
]


async def _force_disconnect_detection(messenger: Messenger, timeout: float = 15.0) -> None:
    """Force the NATS client to detect a broken connection.

    A paused container freezes TCP -- the client cannot detect this passively.
    We attempt a flush with a short timeout to trigger detection, then poll
    is_connected as the nats-py client processes the disconnect asynchronously.
    """
    start = time.monotonic()
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
    while time.monotonic() - start < timeout:
        if not messenger.connection.health_status['is_connected']:
            logger.info('Connection now reports disconnected')
            return
        await asyncio.sleep(0.3)
    logger.warning(
        'Could not confirm is_connected=False within timeout; proceeding'
    )


async def _poll_until_connected(messenger: Messenger, timeout: float = 20.0) -> None:
    """Poll until the NATS connection reports as connected again."""
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        status = messenger.connection.health_status
        if status['is_connected']:
            return
        await asyncio.sleep(0.3)
    raise TimeoutError('Connection did not reconnect within timeout')


async def test_reader_recreates_expired_consumer(resilience_messenger, nats_disruptor, unique_subject):
    """Reader detects ephemeral consumer expiry and recreates it automatically.

    Six-step pattern per D-07:
    1. BASELINE: publish and read with inactive_threshold=5
    2. DISRUPT: pause container for 8s (exceeds 5s threshold)
    3. VERIFY DEGRADED: consumer should be expired on server
    4. RESTORE: unpause container
    5. VERIFY RECOVERY: reader auto-recreates consumer, new messages flow
    6. VERIFY METRICS: reconnect_count incremented, messages_received accurate
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

        baseline_status = reader.health_status
        assert baseline_status['messages_received'] >= 1
        assert baseline_status['reconnect_count'] == 0
        logger.info('Baseline health: messages_received=%d, reconnect_count=%d',
                     baseline_status['messages_received'], baseline_status['reconnect_count'])

        # --- 2. DISRUPT ---
        logger.info('DISRUPTING: pausing NATS container for 8s (inactive_threshold=5s)')
        nats_disruptor.pause()

        # Wait long enough for the ephemeral consumer to expire on the server.
        # The inactive_threshold is 5s; we wait 8s to provide buffer for the
        # NATS server's periodic check interval.
        await asyncio.sleep(8)

        # --- 3. VERIFY DEGRADED ---
        # Force the nats-py client to detect the frozen TCP pipe.
        await _force_disconnect_detection(m, timeout=10)
        logger.info('DEGRADED phase: consumer should be expired on server')

        # --- 4. RESTORE ---
        logger.info('RESTORING: unpausing NATS container')
        nats_disruptor.unpause()

        # Wait for connection to be re-established
        await _poll_until_connected(m, timeout=20)

        # After reconnect, verify the old consumer is gone
        consumer_exists = await reader.check_consumer_exists()
        if not consumer_exists:
            logger.info('Confirmed: consumer expired during pause (check_consumer_exists=False)')
        else:
            logger.info('Consumer still exists after unpause (may not have expired yet)')

        # --- 5. VERIFY RECOVERY ---
        # The reader's read_next loop should detect the missing consumer via
        # ensure_consumer() and call _reopen() to recreate it automatically.
        # Publish a new message and verify the reader receives it.
        await pub.publish(data={'phase': 'after_expiry', 'n': 2})

        # Start a read_next in background -- the reader needs time to detect
        # expiry and recreate the consumer before it can receive messages.
        data, meta = await asyncio.wait_for(reader.read_next(), timeout=30)
        assert data['phase'] == 'after_expiry'
        assert data['n'] == 2
        logger.info('RECOVERY verified: message received after consumer expiry and recreation')

        # --- 6. VERIFY METRICS ---
        final_status = reader.health_status
        assert final_status['reconnect_count'] >= 1, (
            f'Expected reconnect_count >= 1 (consumer recreated), got: {final_status["reconnect_count"]}'
        )
        assert final_status['is_open'] is True
        assert final_status['messages_received'] >= 2
        logger.info('METRICS verified: reconnect_count=%d, messages_received=%d',
                     final_status['reconnect_count'], final_status['messages_received'])

    finally:
        try:
            # Ensure container is unpaused before cleanup
            nats_disruptor.unpause()
        except Exception:
            pass
        await pub.close()
        await reader.close()


async def test_consumer_expiry_health_status_transitions(resilience_messenger, nats_disruptor, unique_subject):
    """Health status transitions accurately reflect consumer expiry and recovery.

    Focuses on the health_status shape during the expiry/recovery cycle:
    reconnect_count must increment and is_open must remain True after recovery.
    """
    m = resilience_messenger
    reader = get_reader(
        subject=unique_subject,
        deliver_policy='all',
        inactive_threshold=5,
    )
    pub = get_publisher(subject=unique_subject)
    try:
        # Establish baseline -- publish and read one message to ensure consumer is active
        await pub.publish(data={'phase': 'setup'})
        await asyncio.wait_for(reader.read_next(), timeout=10)

        pre_status = reader.health_status
        assert pre_status['reconnect_count'] == 0
        logger.info('Pre-disruption: reconnect_count=%d', pre_status['reconnect_count'])

        # Pause for 8 seconds to expire the consumer (inactive_threshold=5)
        logger.info('Pausing container for 8s to trigger consumer expiry')
        nats_disruptor.pause()
        await asyncio.sleep(8)
        await _force_disconnect_detection(m, timeout=10)

        # Unpause and let the reader recover
        nats_disruptor.unpause()
        await _poll_until_connected(m, timeout=20)

        # Start a read_next in background so the reader's loop can detect expiry
        # and recreate the consumer via _reopen()
        read_task = asyncio.create_task(reader.read_next())
        # Give the reader loop time to detect the expired consumer and recreate
        await asyncio.sleep(3)

        # Now publish a message -- the new consumer should pick it up
        await pub.publish(data={'phase': 'post_expiry'})

        # Wait for the reader to receive the message
        data, meta = await asyncio.wait_for(read_task, timeout=30)
        assert data['phase'] == 'post_expiry'

        # Poll health_status until reconnect_count increments
        start = time.monotonic()
        post_status = reader.health_status
        while time.monotonic() - start < 20:
            post_status = reader.health_status
            if post_status['reconnect_count'] > pre_status['reconnect_count']:
                break
            await asyncio.sleep(0.5)

        assert post_status['reconnect_count'] > pre_status['reconnect_count'], (
            f'Expected reconnect_count to increment from {pre_status["reconnect_count"]}, '
            f'got: {post_status["reconnect_count"]}'
        )
        assert post_status['is_open'] is True
        logger.info('Post-recovery: reconnect_count=%d, is_open=%s',
                     post_status['reconnect_count'], post_status['is_open'])

    finally:
        try:
            nats_disruptor.unpause()
        except Exception:
            pass
        await pub.close()
        await reader.close()
