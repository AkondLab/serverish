"""Correctness tests for health_status on all driver types.

Tests health_status properties on readers, publishers, RPC responders,
progress publishers, journal publishers, and connections in both healthy
(open, active) and unhealthy (closed) states.
"""
from __future__ import annotations

import logging

import pytest

from serverish.messenger import get_publisher, get_reader
from serverish.messenger.msg_rpc_resp import MsgRpcResponder, Rpc
from serverish.messenger.msg_progress_pub import get_progresspublisher
from serverish.messenger.msg_journal_pub import get_journalpublisher


# ============ Healthy-state health_status tests (moved from test_messenger_reliability.py) ============


@pytest.mark.nats
async def test_reader_health_status(messenger, unique_subject):
    """Test that reader health_status returns correct values across lifecycle."""
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
    logging.info('Reader health status test passed with %d messages', len(messages))


@pytest.mark.nats
async def test_publisher_health_status(messenger, unique_subject):
    """Test publisher health_status property across lifecycle."""
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
    logging.info('Publisher health status test passed')


@pytest.mark.nats
async def test_rpc_responder_health_status(messenger, unique_subject):
    """Test RPC responder health_status property across lifecycle."""
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
    logging.info('RPC responder health status test passed')


@pytest.mark.nats
async def test_progress_publisher_health_status(messenger, unique_subject):
    """Test progress publisher health_status includes task info."""
    subject = unique_subject

    pub = get_progresspublisher(subject=subject)
    await pub.open()

    # Before adding tasks
    status = pub.health_status
    assert status['active_tasks'] == 0
    assert status['all_done'] is True
    assert status['finished'] is True

    # Add a task
    task_id = await pub.add_task('Test task', total=10)

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
    logging.info('Progress publisher health status test passed')


@pytest.mark.nats
async def test_journal_publisher_health_status(messenger, unique_subject):
    """Test journal publisher health_status includes conversation info."""
    subject = unique_subject

    pub = get_journalpublisher(subject=subject)
    await pub.open()

    # Before logging
    status = pub.health_status
    assert status['active_conversations'] == 0

    # Log a message
    await pub.info('Test message')

    status = pub.health_status
    assert status['publish_count'] >= 1

    await pub.close()
    logging.info('Journal publisher health status test passed')


@pytest.mark.nats
async def test_connection_health_status(messenger):
    """Test connection health_status includes slow consumer tracking."""
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

    logging.info('Connection health status test passed')


# ============ Unhealthy-state (closed) health_status tests ============


@pytest.mark.nats
async def test_reader_health_status_closed(messenger, unique_subject):
    """Test that reader health_status shows is_open=False after close."""
    subject = unique_subject

    await messenger.purge(subject)

    reader = get_reader(subject=subject, deliver_policy='new', nowait=True)
    await reader.open()

    status = reader.health_status
    assert status['is_open'] is True

    await reader.close()

    status = reader.health_status
    assert status['is_open'] is False

    logging.info('Reader health status closed test passed')


@pytest.mark.nats
async def test_publisher_health_status_closed(messenger, unique_subject):
    """Test that publisher health_status retains counts after close but shows is_open=False."""
    subject = unique_subject

    pub = get_publisher(subject=subject)
    await pub.open()
    await pub.publish(data={'test': 'data'})

    # Verify publish count before close
    status = pub.health_status
    assert status['is_open'] is True
    assert status['publish_count'] == 1

    await pub.close()

    # After close: is_open should be False, but publish_count retained
    status = pub.health_status
    assert status['is_open'] is False
    assert status['publish_count'] == 1

    logging.info('Publisher health status closed test passed')
