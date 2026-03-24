"""Tests for messenger reliability improvements

Tests the new reliability features:
- Proactive consumer health checks
- Reconnection event handling
- Health status monitoring (readers and publishers)
- RPC responder auto-resubscribe
- Publisher health tracking
"""

import asyncio
import logging
import time

import pytest

from serverish.messenger import Messenger, get_publisher, get_reader
from serverish.messenger.msg_rpc_resp import MsgRpcResponder, Rpc
from serverish.messenger.msg_progress_pub import get_progresspublisher
from serverish.messenger.msg_journal_pub import get_journalpublisher


@pytest.mark.nats
async def test_health_status_property(messenger, unique_subject):
    """Test that health_status property returns correct values"""
    subject = unique_subject

    await messenger.purge(subject)

    # Publish some messages
    pub = get_publisher(subject=subject)
    await pub.publish(data={'n': 1})
    await pub.publish(data={'n': 2})
    await pub.close()

    # Create reader and check health status before opening
    reader = get_reader(subject=subject, deliver_policy='all', nowait=True)

    # Before opening - health status should show not open
    status = reader.health_status
    assert status['is_open'] is False
    assert status['messages_received'] == 0
    assert status['reconnect_count'] == 0
    assert status['last_message_time'] is None
    assert status['last_error'] is None

    # Open and read messages
    await reader.open()

    # After opening but before reading
    status = reader.health_status
    assert status['is_open'] is True
    assert status['subject'] == subject

    # Read messages
    messages = []
    async for data, meta in reader:
        messages.append(data)

    # After reading - health status should show messages received
    status = reader.health_status
    assert status['messages_received'] == 2
    assert status['last_message_time'] is not None
    assert status['last_message_ago'] is not None
    assert status['last_message_ago'] < 5.0  # Should be recent
    # Check new pending/slow consumer fields
    assert 'pending_messages' in status
    assert 'pending_bytes' in status
    assert 'connection_slow_consumers' in status

    await reader.close()
    logging.info(f"Health status test passed with {len(messages)} messages")


@pytest.mark.nats
async def test_check_consumer_exists(messenger, unique_subject):
    """Test the check_consumer_exists method"""
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

    logging.info("Consumer exists check test passed")


@pytest.mark.nats
async def test_health_check_mechanism(messenger, unique_subject):
    """Test that health check mechanism is properly configured"""
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
    # The health check runs every 10 seconds, so it won't run immediately
    # with nowait=True (test completes too fast)
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
    logging.info("Health check mechanism test passed")


@pytest.mark.nats
async def test_rpc_responder_health_status(messenger, unique_subject):
    """Test RPC responder health status property"""
    subject = unique_subject

    responder = MsgRpcResponder(subject=subject, parent=messenger)

    # Before open
    status = responder.health_status
    assert status['is_open'] is False
    assert status['subject'] == subject
    assert status['has_subscription'] is False
    assert status['reconnect_count'] == 0

    # Open and register function
    await responder.open()

    def callback(rpc: Rpc):
        rpc.set_response(data={'result': 'ok'})

    await responder.register_function(callback)

    # After open
    status = responder.health_status
    assert status['is_open'] is True
    assert status['has_subscription'] is True
    # Check new pending/slow consumer fields
    assert 'pending_messages' in status
    assert 'pending_bytes' in status
    assert 'connection_slow_consumers' in status

    await responder.close()
    logging.info("RPC responder health status test passed")


@pytest.mark.nats
async def test_reconnect_count_tracking(messenger, unique_subject):
    """Test that reconnect count is tracked properly"""
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
    logging.info("Reconnect count tracking test passed")


@pytest.mark.nats
async def test_inactive_threshold_default(messenger, unique_subject):
    """Test that the default inactive_threshold is 300 seconds"""
    subject = unique_subject

    reader = get_reader(subject=subject, deliver_policy='new')

    # Check the consumer config defaults
    assert reader.consumer_cfg.get('inactive_threshold') == 300

    logging.info("Inactive threshold default test passed")


# ============ Publisher Health Tests ============

@pytest.mark.nats
async def test_publisher_health_status(messenger, unique_subject):
    """Test publisher health_status property"""
    subject = unique_subject

    pub = get_publisher(subject=subject)

    # Before opening - health status should show defaults
    status = pub.health_status
    assert status['is_open'] is False
    assert status['subject'] == subject
    assert status['publish_count'] == 0
    assert status['error_count'] == 0
    assert status['last_publish_time'] is None
    assert status['last_error'] is None

    # Open and publish
    await pub.open()
    await pub.publish(data={'test': 'data1'})
    await pub.publish(data={'test': 'data2'})

    # After publishing
    status = pub.health_status
    assert status['is_open'] is True
    assert status['publish_count'] == 2
    assert status['error_count'] == 0
    assert status['last_publish_time'] is not None
    assert status['last_publish_ago'] is not None
    assert status['last_publish_ago'] < 5.0  # Should be recent

    await pub.close()
    logging.info("Publisher health status test passed")


@pytest.mark.nats
async def test_progress_publisher_health_status(messenger, unique_subject):
    """Test progress publisher health_status includes task info"""
    subject = unique_subject

    pub = get_progresspublisher(subject=subject)
    await pub.open()

    # Before adding tasks
    status = pub.health_status
    assert status['active_tasks'] == 0
    assert status['all_done'] is True
    assert status['finished'] is True

    # Add a task
    task_id = await pub.add_task("Test task", total=10)

    status = pub.health_status
    assert status['active_tasks'] == 1
    assert status['all_done'] is False
    assert status['finished'] is False
    assert status['publish_count'] >= 1  # At least one publish for add_task

    # Complete the task
    await pub.update(task_id, completed=10)

    status = pub.health_status
    assert status['all_done'] is True
    assert status['finished'] is True

    await pub.close()
    logging.info("Progress publisher health status test passed")


@pytest.mark.nats
async def test_journal_publisher_health_status(messenger, unique_subject):
    """Test journal publisher health_status includes conversation info"""
    subject = unique_subject

    pub = get_journalpublisher(subject=subject)
    await pub.open()

    # Before logging
    status = pub.health_status
    assert status['active_conversations'] == 0

    # Log a message
    await pub.info("Test message")

    status = pub.health_status
    assert status['publish_count'] >= 1

    await pub.close()
    logging.info("Journal publisher health status test passed")


@pytest.mark.nats
async def test_connection_health_status(messenger):
    """Test connection health_status includes slow consumer tracking"""
    conn = messenger.connection

    # Check connection health status fields
    status = conn.health_status
    assert 'is_connected' in status
    assert 'slow_consumer_count' in status
    assert 'last_slow_consumer_time' in status
    assert 'last_slow_consumer_ago' in status
    assert 'error_count' in status
    assert 'last_error' in status

    # Should be connected with no slow consumers initially
    assert status['is_connected'] is True
    assert status['slow_consumer_count'] == 0
    assert status['last_slow_consumer_time'] is None

    logging.info("Connection health status test passed")
