"""Publisher error/success tracking correctness tests (CORR-05).

Verifies that publish_count, error_count, last_error, and timing fields
reflect actual publish outcomes under normal conditions.
"""
from __future__ import annotations

import logging

import pytest

from serverish.messenger import get_publisher

logger = logging.getLogger(__name__)


@pytest.mark.nats
async def test_publisher_success_tracking(messenger, unique_subject):
    """Verify publish_count and timing update after successful publishes."""
    pub = get_publisher(subject=unique_subject)
    try:
        # Before open: defaults
        assert pub.health_status['publish_count'] == 0
        assert pub.health_status['last_publish_time'] is None

        await pub.open()

        # Publish 3 messages
        for i in range(3):
            await pub.publish(data={'n': i})

        # After publishing: count and timing updated
        status = pub.health_status
        assert status['publish_count'] == 3
        assert status['last_publish_time'] is not None
        assert status['last_publish_ago'] < 5.0
    finally:
        await pub.close()

    logger.info('Publisher success tracking test passed')


@pytest.mark.nats
async def test_publisher_error_fields_default(messenger, unique_subject):
    """Verify error tracking fields exist and default correctly after success."""
    pub = get_publisher(subject=unique_subject)
    try:
        await pub.open()
        await pub.publish(data={'value': 42})

        # After successful publish: error fields remain at defaults
        status = pub.health_status
        assert status['error_count'] == 0
        assert status['last_error'] is None
    finally:
        await pub.close()

    logger.info('Publisher error fields default test passed')


@pytest.mark.nats
async def test_publisher_publish_count_increments(messenger, unique_subject):
    """Verify publish_count increments per publish, checked incrementally."""
    pub = get_publisher(subject=unique_subject)
    try:
        await pub.open()

        for expected in range(1, 6):
            await pub.publish(data={'n': expected})
            assert pub.health_status['publish_count'] == expected
    finally:
        await pub.close()

    logger.info('Publisher publish count increments test passed')
