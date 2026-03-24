"""Consumer lifecycle correctness tests (CORR-04).

Verifies consumer state transitions: not-open -> open -> read -> close,
check_consumer_exists at each stage, health check mechanism internals,
inactive threshold defaults, and reconnect count tracking.
"""
from __future__ import annotations

import logging

import pytest

from serverish.messenger import get_publisher, get_reader

logger = logging.getLogger(__name__)


@pytest.mark.nats
async def test_consumer_lifecycle_full(messenger, unique_subject):
    """Test the complete consumer state machine: not-open -> open -> read -> close."""
    subject = unique_subject

    # Setup: publish 1 message
    pub = get_publisher(subject=subject)
    await pub.publish(data={'n': 1})
    await pub.close()

    reader = get_reader(subject=subject, deliver_policy='all', nowait=True)
    try:
        # Phase 1 (before open): not open, consumer does not exist
        assert reader.is_open is False
        assert await reader.check_consumer_exists() is False

        # Phase 2 (after open): open, consumer exists
        await reader.open()
        assert reader.is_open is True
        assert await reader.check_consumer_exists() is True

        # Phase 3 (after read): message received reflected in health_status
        async for data, meta in reader:
            pass
        assert reader.health_status['messages_received'] == 1

        # Phase 4 (after close): not open, consumer does not exist
        await reader.close()
        assert reader.is_open is False
        assert await reader.check_consumer_exists() is False
    finally:
        if reader.is_open:
            await reader.close()

    logger.info('Consumer lifecycle full test passed')


@pytest.mark.nats
async def test_check_consumer_exists(messenger, unique_subject):
    """Test the check_consumer_exists method."""
    subject = unique_subject

    await messenger.purge(subject)

    reader = get_reader(subject=subject, deliver_policy='new', nowait=True)

    # Before open - should be False
    exists = await reader.check_consumer_exists()
    assert exists is False

    # After open - should be True
    await reader.open()
    exists = await reader.check_consumer_exists()
    assert exists is True

    await reader.close()

    # After close - should be False
    exists = await reader.check_consumer_exists()
    assert exists is False

    logger.info('Consumer exists check test passed')


@pytest.mark.nats
async def test_health_check_mechanism(messenger, unique_subject):
    """Test that health check mechanism is properly configured."""
    subject = unique_subject

    await messenger.purge(subject)

    # Publish a message
    pub = get_publisher(subject=subject)
    await pub.publish(data={'test': 'data'})
    await pub.close()

    # Read with nowait
    reader = get_reader(subject=subject, deliver_policy='all', nowait=True)
    await reader.open()

    # Verify the health check mechanism fields exist and are initialized
    assert hasattr(reader, '_last_health_check_time')
    assert hasattr(reader, '_message_count')
    assert hasattr(reader, '_reconnect_count')
    assert reader._message_count == 0  # Before reading

    # Read the message
    async for data, meta in reader:
        pass

    # After reading, message count should be updated
    assert reader._message_count == 1
    assert reader._last_message_time is not None

    await reader.close()
    logger.info('Health check mechanism test passed')


@pytest.mark.nats
async def test_inactive_threshold_default(messenger, unique_subject):
    """Test that the default inactive_threshold is 300 seconds."""
    subject = unique_subject

    reader = get_reader(subject=subject, deliver_policy='new')

    # Check the consumer config defaults
    assert reader.consumer_cfg.get('inactive_threshold') == 300

    logger.info('Inactive threshold default test passed')


@pytest.mark.nats
async def test_reconnect_count_tracking(messenger, unique_subject):
    """Test that reconnect count is tracked properly."""
    subject = unique_subject

    await messenger.purge(subject)

    # Publish messages
    pub = get_publisher(subject=subject)
    for i in range(5):
        await pub.publish(data={'n': i})
    await pub.close()

    reader = get_reader(subject=subject, deliver_policy='all', nowait=True)
    await reader.open()

    # Initial reconnect count should be 0
    assert reader._reconnect_count == 0

    # Read messages
    async for data, meta in reader:
        pass

    # Reconnect count should still be 0 (no reconnection needed)
    assert reader._reconnect_count == 0

    await reader.close()
    logger.info('Reconnect count tracking test passed')
