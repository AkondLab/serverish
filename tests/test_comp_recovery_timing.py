"""Comparative tests: recovery timing benchmarks (COMP-03).

Measures disconnect-to-first-message recovery time for both simple reconnect
and consumer expiry scenarios. Asserts recovery within 15 seconds on
fixsubscriptions. On master, gracefully xfails via pytest.xfail when the
recovery read times out, preventing CI hangs.

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
    pytest.mark.timeout(90),
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


async def test_recovery_timing_after_disconnect(
    resilience_messenger, nats_disruptor, unique_subject,
):
    """Measure disconnect-to-first-message recovery time after simple reconnect.

    Benchmarks the time from container unpause to first successfully received
    message. On fixsubscriptions, asserts recovery within 15 seconds.
    On master, gracefully xfails if the recovery read times out.
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

        # --- 2. DISRUPT ---
        logger.info('DISRUPTING: pausing NATS container')
        nats_disruptor.pause()
        await asyncio.sleep(3)
        await _force_disconnect_detection(m, timeout=10)

        # --- 3. RECORD RESTORE TIME ---
        restore_time = time.monotonic()
        logger.info('RESTORING: unpausing NATS container')
        nats_disruptor.unpause()

        # --- 4. WAIT FOR CONNECTION ---
        await _poll_until_connected(m, timeout=20)

        # --- 5. PUBLISH AND MEASURE ---
        published = False
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

        try:
            data, meta = await asyncio.wait_for(reader.read_next(), timeout=25)
            first_message_time = time.monotonic()
            recovery_duration = first_message_time - restore_time
        except asyncio.TimeoutError:
            if is_master:
                pytest.xfail('Master fails to recover within benchmark timeout')
            raise AssertionError('Fixsubscriptions failed to recover within 25s -- unexpected')

        # --- 6. ASSERT AND LOG ---
        logger.info('Recovery timing: %.2fs (from unpause to first message)', recovery_duration)
        assert recovery_duration < 15.0, (
            f'Recovery took {recovery_duration:.2f}s, exceeds 15s threshold'
        )

    finally:
        try:
            nats_disruptor.unpause()
        except Exception:
            pass
        await pub.close()
        await reader.close()


async def test_recovery_timing_after_consumer_expiry(
    resilience_messenger, nats_disruptor, unique_subject,
):
    """Measure recovery time after consumer expiry (inactive_threshold exceeded).

    Same pattern as disconnect test, but with consumer expiry via
    inactive_threshold=5 and an 8-second pause. This is a harder recovery
    scenario because the consumer must be recreated, not just reconnected.
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
        logger.info('BASELINE passed: message received normally')

        # --- 2. DISRUPT ---
        logger.info('DISRUPTING: pausing NATS container for 8s (inactive_threshold=5s)')
        nats_disruptor.pause()
        await asyncio.sleep(8)

        # --- 3. RECORD RESTORE TIME ---
        await _force_disconnect_detection(m, timeout=10)
        restore_time = time.monotonic()
        logger.info('RESTORING: unpausing NATS container')
        nats_disruptor.unpause()

        # --- 4. WAIT FOR CONNECTION ---
        await _poll_until_connected(m, timeout=20)

        # --- 5. PUBLISH AND MEASURE ---
        published = False
        for attempt in range(20):
            try:
                await pub.publish(data={'phase': 'after_expiry', 'n': 2})
                published = True
                logger.info('Published after expiry recovery (attempt %d)', attempt + 1)
                break
            except Exception as e:
                logger.debug('Publish attempt %d failed: %s', attempt + 1, e)
                await asyncio.sleep(0.5)
        assert published, 'Could not publish after expiry recovery within retry limit'

        try:
            data, meta = await asyncio.wait_for(reader.read_next(), timeout=25)
            first_message_time = time.monotonic()
            recovery_duration = first_message_time - restore_time
        except asyncio.TimeoutError:
            if is_master:
                pytest.xfail('Master fails to recover within benchmark timeout')
            raise AssertionError('Fixsubscriptions failed to recover within 25s -- unexpected')

        # --- 6. ASSERT AND LOG ---
        logger.info('Recovery timing (consumer expiry): %.2fs (from unpause to first message)',
                     recovery_duration)
        assert recovery_duration < 15.0, (
            f'Recovery took {recovery_duration:.2f}s, exceeds 15s threshold'
        )

        if is_fixsubscriptions():
            reader_status = reader.health_status
            logger.info('Recovery metrics: reconnect_count=%d, messages_received=%d',
                         reader_status['reconnect_count'], reader_status['messages_received'])

    finally:
        try:
            nats_disruptor.unpause()
        except Exception:
            pass
        await pub.close()
        await reader.close()
