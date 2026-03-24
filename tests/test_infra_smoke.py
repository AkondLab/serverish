"""Infrastructure smoke tests.

Validates that the test infrastructure (testcontainers, fixtures,
subject isolation, and NatsDisruptor) all work correctly.
"""
from __future__ import annotations

import asyncio
import re
import uuid

import pytest

from serverish.messenger import Messenger, get_publisher, get_reader


@pytest.mark.nats
def test_nats_server_available(nats_server):
    """INFR-01/INFR-03: NATS server fixture provides valid connection info."""
    assert isinstance(nats_server['host'], str)
    assert isinstance(nats_server['port'], int)
    assert nats_server['port'] > 0


@pytest.mark.nats
async def test_messenger_connected(messenger):
    """Session-scoped Messenger is connected and ready."""
    assert messenger.is_open is True


@pytest.mark.nats
def test_unique_subject_format(unique_subject):
    """INFR-04: unique_subject fixture produces correctly formatted subjects."""
    assert unique_subject.startswith('test.')
    parts = unique_subject.split('.')
    assert len(parts) == 3
    assert re.match(r'^[0-9a-f]{8}$', parts[2])


@pytest.mark.nats
def test_unique_subjects_differ():
    """unique_subject logic produces different values on each call."""
    uid_a = uuid.uuid4().hex[:8]
    uid_b = uuid.uuid4().hex[:8]
    assert uid_a != uid_b


@pytest.mark.nats
async def test_publish_subscribe_with_fixtures(messenger, unique_subject):
    """End-to-end smoke test: publish a message and read it back."""
    # Ensure the unique subject is routed to a JetStream stream.
    # Use the 'test' stream with wildcard 'test.>' if it exists,
    # otherwise create a dedicated stream for test infrastructure.
    stream_name = 'test'
    try:
        await messenger.connection.ensure_subject_in_stream(
            stream_name, unique_subject, create_stram_if_needed=True,
        )
    except Exception:
        # Subject may already be covered by a wildcard in the stream
        pass

    pub = get_publisher(subject=unique_subject)
    reader = get_reader(subject=unique_subject, deliver_policy='all', nowait=True)

    try:
        await pub.publish(data={'test': 'smoke'})

        # Small delay to allow message to be persisted in JetStream
        await asyncio.sleep(0.5)

        found = False
        async for data, meta in reader:
            assert data['test'] == 'smoke'
            found = True
            break
        assert found, 'Expected to read back the published message'
    finally:
        await pub.close()
        await reader.close()


@pytest.mark.nats
def test_disruptor_pause_unpause(nats_disruptor):
    """INFR-07: NatsDisruptor fixture provides working container controls."""
    assert isinstance(nats_disruptor.host, str)
    assert isinstance(nats_disruptor.port, int)
    # Verify pause/unpause cycle completes without error
    nats_disruptor.pause()
    nats_disruptor.unpause()
