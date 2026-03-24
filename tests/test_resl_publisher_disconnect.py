"""Resilience tests: publisher behavior during NATS disconnect (RESL-04).

Proves that MsgPublisher tracks errors when publishing during a disconnect
and resumes successful publishing after reconnection, with error_count
and last_error accurately reflecting what happened.
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
    pytest.mark.timeout(60),
]


async def _force_disconnect_detection(messenger: Messenger, timeout: float = 15.0) -> None:
    """Force the NATS client to detect a broken connection."""
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


async def test_publisher_tracks_errors_during_disconnect(resilience_messenger, nats_disruptor, unique_subject):
    """Publisher tracks errors during disconnect and resumes after reconnection.

    Six-step pattern per D-07:
    1. BASELINE: publish successfully, verify counts
    2. DISRUPT: pause container
    3. VERIFY DEGRADED: publish attempt increments error_count
    4. RESTORE: unpause container
    5. VERIFY RECOVERY: publishing succeeds again
    6. VERIFY METRICS: error_count, publish_count, last_error accurate
    """
    m = resilience_messenger
    pub = get_publisher(subject=unique_subject)
    pub.raise_on_publish_error = False
    try:
        # --- 1. BASELINE ---
        await pub.publish(data={'phase': 'baseline'})
        baseline_status = pub.health_status
        assert baseline_status['publish_count'] >= 1
        assert baseline_status['error_count'] == 0
        assert baseline_status['last_error'] is None
        logger.info('BASELINE passed: publish_count=%d, error_count=%d',
                     baseline_status['publish_count'], baseline_status['error_count'])

        # --- 2. DISRUPT ---
        logger.info('DISRUPTING: pausing NATS container')
        nats_disruptor.pause()
        await asyncio.sleep(3)

        # Force disconnect detection so the client knows the connection is broken
        await _force_disconnect_detection(m, timeout=10)

        # --- 3. VERIFY DEGRADED ---
        # Attempt to publish during disconnect; with raise_on_publish_error=False
        # this should not raise but should increment error_count.
        await pub.publish(data={'phase': 'during_disconnect'})
        degraded_status = pub.health_status
        assert degraded_status['error_count'] >= 1, (
            f'Expected error_count >= 1 after publish during disconnect, got: {degraded_status["error_count"]}'
        )
        assert degraded_status['last_error'] is not None, (
            f'Expected last_error to be set after publish failure'
        )
        error_count_after_disconnect = degraded_status['error_count']
        logger.info('DEGRADED verified: error_count=%d, last_error=%s',
                     degraded_status['error_count'], degraded_status['last_error'])

        # --- 4. RESTORE ---
        logger.info('RESTORING: unpausing NATS container')
        nats_disruptor.unpause()
        await _poll_until_connected(m, timeout=20)

        # --- 5. VERIFY RECOVERY ---
        # Poll until publish succeeds (no new errors)
        recovered = False
        start = time.monotonic()
        while time.monotonic() - start < 20:
            try:
                await pub.publish(data={'phase': 'recovery'})
                recovery_status = pub.health_status
                # If publish_count increased and no new errors, we've recovered
                if recovery_status['error_count'] == error_count_after_disconnect:
                    recovered = True
                    break
            except Exception as e:
                logger.debug('Publish attempt failed during recovery: %s', e)
            await asyncio.sleep(0.5)
        assert recovered, 'Publisher did not recover within 20s'
        logger.info('RECOVERY verified: publishing resumed successfully')

        # --- 6. VERIFY METRICS ---
        final_status = pub.health_status
        assert final_status['publish_count'] > baseline_status['publish_count'], (
            f'Expected publish_count > {baseline_status["publish_count"]}, got: {final_status["publish_count"]}'
        )
        assert final_status['error_count'] >= 1, (
            f'Expected error_count >= 1, got: {final_status["error_count"]}'
        )
        # Note: publisher is_open tracks the context-manager lifecycle, not connection state.
        # The publisher uses @ensure_open for one-shot operations, so is_open may be False
        # between publish calls. The critical proof is that publish_count and error_count are accurate.
        logger.info('METRICS verified: publish_count=%d, error_count=%d, last_error=%s',
                     final_status['publish_count'], final_status['error_count'],
                     final_status['last_error'])

    finally:
        try:
            nats_disruptor.unpause()
        except Exception:
            pass
        await pub.close()


async def test_publisher_raises_during_disconnect(resilience_messenger, nats_disruptor, unique_subject):
    """Publisher raises exception during disconnect when raise_on_publish_error=True.

    Verifies the default behavior where publish errors are raised as exceptions,
    and that publishing resumes after reconnection.
    """
    m = resilience_messenger
    pub = get_publisher(subject=unique_subject)
    # Default raise_on_publish_error=True, but be explicit
    pub.raise_on_publish_error = True
    try:
        # --- 1. BASELINE ---
        await pub.publish(data={'phase': 'baseline'})
        assert pub.health_status['publish_count'] >= 1
        logger.info('BASELINE passed: publish succeeded')

        # --- 2. DISRUPT ---
        logger.info('DISRUPTING: pausing NATS container')
        nats_disruptor.pause()
        await asyncio.sleep(3)
        await _force_disconnect_detection(m, timeout=10)

        # --- 3. VERIFY DEGRADED ---
        # With raise_on_publish_error=True, publishing should raise an exception
        with pytest.raises(Exception) as exc_info:
            await pub.publish(data={'phase': 'during_disconnect'})
        logger.info('DEGRADED verified: publish raised %s: %s',
                     type(exc_info.value).__name__, exc_info.value)

        assert pub.health_status['error_count'] >= 1

        # --- 4. RESTORE ---
        logger.info('RESTORING: unpausing NATS container')
        nats_disruptor.unpause()
        await _poll_until_connected(m, timeout=20)

        # --- 5. VERIFY RECOVERY ---
        recovered = False
        start = time.monotonic()
        while time.monotonic() - start < 20:
            try:
                await pub.publish(data={'phase': 'recovery'})
                recovered = True
                break
            except Exception as e:
                logger.debug('Publish attempt failed during recovery: %s', e)
                await asyncio.sleep(0.5)
        assert recovered, 'Publisher did not recover within 20s'
        logger.info('RECOVERY verified: publishing resumed after exception')

        # --- 6. VERIFY METRICS ---
        final_status = pub.health_status
        assert final_status['error_count'] >= 1
        assert final_status['publish_count'] >= 2  # baseline + recovery
        logger.info('METRICS verified: publish_count=%d, error_count=%d',
                     final_status['publish_count'], final_status['error_count'])

    finally:
        try:
            nats_disruptor.unpause()
        except Exception:
            pass
        await pub.close()
