"""Test nowait functionality for MsgReader"""
import logging
import asyncio
import pytest

from serverish.messenger import Messenger, get_publisher, get_reader
from tests.test_connection import ci
from tests.test_nats import is_nats_running

log = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_nowait_with_messages():
    """Test that nowait=True returns all available messages without hanging"""
    subject = 'test.messenger.nowait_with_messages'

    async with Messenger().context(host='localhost', port=4222) as messenger:
        # Publish some test messages
        pub = get_publisher(subject=subject)
        await messenger.purge(subject)

        num_messages = 25
        for i in range(num_messages):
            await pub.publish(data={'index': i, 'message': f'test_{i}'})

        await asyncio.sleep(0.1)  # Let messages settle

        # Read with nowait=True
        reader = get_reader(subject=subject, deliver_policy='all', nowait=True)

        received = []
        start = asyncio.get_event_loop().time()
        async for data, meta in reader:
            received.append(data)
        end = asyncio.get_event_loop().time()

        await reader.close()

        # Verify we got all messages
        assert len(received) == num_messages, f"Expected {num_messages} messages, got {len(received)}"

        # Verify we didn't hang (should complete quickly)
        elapsed = end - start
        assert elapsed < 15.0, f"nowait=True took {elapsed:.1f}s, should be < 15s"

        # Verify message content
        for i, data in enumerate(received):
            assert data['index'] == i, f"Message {i} has wrong index: {data['index']}"

        log.info(f"✓ Successfully read {len(received)} messages in {elapsed:.2f}s with nowait=True")


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_nowait_empty_subject():
    """Test that nowait=True returns immediately when no messages exist"""
    subject = 'test.messenger.nowait_empty'

    async with Messenger().context(host='localhost', port=4222) as messenger:
        # Ensure subject is empty
        await messenger.purge(subject)
        await asyncio.sleep(0.1)

        # Read with nowait=True
        reader = get_reader(subject=subject, deliver_policy='all', nowait=True)

        received = []
        start = asyncio.get_event_loop().time()
        async for data, meta in reader:
            received.append(data)
        end = asyncio.get_event_loop().time()

        await reader.close()

        # Verify we got no messages
        assert len(received) == 0, f"Expected 0 messages from empty subject, got {len(received)}"

        # Verify we returned quickly (not waiting 100s timeout)
        elapsed = end - start
        assert elapsed < 15.0, f"nowait=True on empty subject took {elapsed:.1f}s, should be < 15s"

        log.info(f"✓ Empty subject returned immediately in {elapsed:.2f}s with nowait=True")


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_nowait_large_batch():
    """Test that nowait=True handles large message batches correctly"""
    subject = 'test.messenger.nowait_large_batch'

    async with Messenger().context(host='localhost', port=4222) as messenger:
        # Publish many messages (more than default batch size of 100)
        pub = get_publisher(subject=subject)
        await messenger.purge(subject)

        num_messages = 250
        for i in range(num_messages):
            await pub.publish(data={'index': i})

        await asyncio.sleep(0.2)  # Let messages settle

        # Read with nowait=True
        reader = get_reader(subject=subject, deliver_policy='all', nowait=True)

        received = []
        start = asyncio.get_event_loop().time()
        async for data, meta in reader:
            received.append(data)
        end = asyncio.get_event_loop().time()

        await reader.close()

        # Verify we got all messages
        assert len(received) == num_messages, f"Expected {num_messages} messages, got {len(received)}"

        elapsed = end - start
        assert elapsed < 20.0, f"nowait=True with {num_messages} messages took {elapsed:.1f}s, should be < 20s"

        log.info(f"✓ Successfully read {len(received)} messages in {elapsed:.2f}s with nowait=True")


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_nowait_false_waits():
    """Test that nowait=False waits for new messages"""
    subject = 'test.messenger.nowait_false_waits'

    async with Messenger().context(host='localhost', port=4222) as messenger:
        await messenger.purge(subject)
        await asyncio.sleep(0.1)

        # Start reader with nowait=False in background
        reader = get_reader(subject=subject, deliver_policy='all', nowait=False)

        received = []

        async def reader_task():
            async for data, meta in reader:
                received.append(data)
                if data.get('finish'):
                    reader.stop()
                    break

        task = asyncio.create_task(reader_task())

        # Wait a bit to ensure reader is waiting
        await asyncio.sleep(0.5)

        # Now publish a message
        pub = get_publisher(subject=subject)
        await pub.publish(data={'index': 0, 'finish': True})

        # Wait for reader to get it
        await asyncio.wait_for(task, timeout=5.0)
        await reader.close()

        # Verify we got the message
        assert len(received) == 1, f"Expected 1 message, got {len(received)}"
        assert received[0]['index'] == 0

        log.info(f"✓ nowait=False correctly waited for new message")


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_nowait_with_deliver_policy_last():
    """Test nowait with deliver_policy='last'"""
    subject = 'test.messenger.nowait_last'

    async with Messenger().context(host='localhost', port=4222) as messenger:
        # Publish several messages
        pub = get_publisher(subject=subject)
        await messenger.purge(subject)

        for i in range(10):
            await pub.publish(data={'index': i})

        await asyncio.sleep(0.1)

        # Read with deliver_policy='last' and nowait=True
        reader = get_reader(subject=subject, deliver_policy='last', nowait=True)

        received = []
        async for data, meta in reader:
            received.append(data)

        await reader.close()

        # Should only get the last message
        assert len(received) == 1, f"Expected 1 message with deliver_policy='last', got {len(received)}"
        assert received[0]['index'] == 9, f"Expected last message (index=9), got {received[0]}"

        log.info(f"✓ deliver_policy='last' with nowait=True returned only last message")
