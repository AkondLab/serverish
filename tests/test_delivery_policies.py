import asyncio
import datetime
import logging
import uuid

import pytest

from serverish.messenger import Messenger, get_publisher, get_reader
from tests.test_connection import ci
from tests.test_nats import is_nats_running


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_delivery_policy_all():
    """Test that 'all' delivery policy retrieves all messages from the stream."""
    subject = f'test.messenger.delivery_policy.all.{uuid.uuid4()}'
    collected = []

    async with Messenger().context(host='localhost', port=4222) as mess:
        # Ensure clean stream
        await mess.purge(subject)

        # Publish 5 messages
        pub = get_publisher(subject)
        for i in range(5):
            await pub.publish(data={'index': i, 'final': i == 4})
            await asyncio.sleep(0.01)

        # Read with 'all' policy
        sub = get_reader(subject, deliver_policy='all')
        async for msg, _ in sub:
            collected.append(msg)
            if msg.get('final'):
                break

        await pub.close()
        await sub.close()

    # Should get all 5 messages
    assert len(collected) == 5
    assert [msg['index'] for msg in collected] == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_delivery_policy_last():
    """Test that 'last' delivery policy retrieves only the last message from the stream."""
    subject = f'test.messenger.delivery_policy.last.{uuid.uuid4()}'
    collected = []

    async with Messenger().context(host='localhost', port=4222) as mess:
        # Ensure clean stream
        await mess.purge(subject)

        # Publish 5 messages
        pub = get_publisher(subject)
        for i in range(5):
            await pub.publish(data={'index': i})
            await asyncio.sleep(0.01)

        # Read with 'last' policy
        sub = get_reader(subject, deliver_policy='last')

        # Only one message should be delivered
        async for msg, _ in sub:
            collected.append(msg)
            break  # We expect only one message anyway

        # Publish one more message to confirm 'last' behavior
        await pub.publish(data={'index': 5})

        # Create new reader with 'last' policy
        sub2 = get_reader(subject, deliver_policy='last')
        msg, _ = await sub2.__anext__()
        collected.append(msg)

        await pub.close()
        await sub.close()
        await sub2.close()

    # Should get only the last message from each subscription attempt
    assert len(collected) == 2
    assert collected[0]['index'] == 4  # Last message from first batch
    assert collected[1]['index'] == 5  # Last message after additional publish


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_delivery_policy_new():
    """Test that 'new' delivery policy retrieves only new messages published after subscription."""
    subject = f'test.messenger.delivery_policy.new.{uuid.uuid4()}'
    collected = []

    async with Messenger().context(host='localhost', port=4222) as mess:
        # Ensure clean stream
        await mess.purge(subject)

        # Publish 5 messages before subscription
        pub = get_publisher(subject)
        for i in range(5):
            await pub.publish(data={'index': i, 'batch': 'pre'})
            await asyncio.sleep(0.01)

        # Create subscription with 'new' policy
        sub = get_reader(subject, deliver_policy='new')

        async def publish_3_more():
            # Publish 3 new messages after subscription
            await asyncio.sleep(0.02)
            for i in range(3):
                await pub.publish(data={'index': i, 'batch': 'post'})
                await asyncio.sleep(0.01)

        async def read_3():
            # Read messages - should only get new ones
            async for msg, _ in sub:
                collected.append(msg)
                if len(collected) >= 3:  # We expect only 3 messages
                    break

        task_publish_3_more = asyncio.create_task(publish_3_more())
        task_read_3 = asyncio.create_task(read_3())
        await asyncio.gather(task_publish_3_more, task_read_3)

        await pub.close()
        await sub.close()

    # Should only get messages published after subscription
    assert len(collected) == 3
    assert all(msg['batch'] == 'post' for msg in collected)
    assert [msg['index'] for msg in collected] == [0, 1, 2]


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_delivery_policy_by_start_time():
    """Test that 'by_start_time' delivery policy retrieves messages published after specific time."""
    subject = f'test.messenger.delivery_policy.time.{uuid.uuid4()}'
    collected = []

    async with Messenger().context(host='localhost', port=4222) as mess:
        # Ensure clean stream
        await mess.purge(subject)

        # Publish 5 messages before timestamp
        pub = get_publisher(subject)
        for i in range(5):
            await pub.publish(data={'index': i, 'batch': 'pre'})
            await asyncio.sleep(0.01)

        # Record time for filtering
        timestamp = datetime.datetime.now()
        await asyncio.sleep(0.1)  # Ensure separation

        # Publish 3 messages after timestamp
        for i in range(3):
            await pub.publish(data={'index': i, 'batch': 'post'})
            await asyncio.sleep(0.01)

        # Read with time-based policy
        sub = get_reader(subject, deliver_policy='by_start_time', opt_start_time=timestamp)

        async for msg, _ in sub:
            collected.append(msg)
            if len(collected) >= 3:  # We expect only 3 messages
                break

        await pub.close()
        await sub.close()

    # Should only get messages published after timestamp
    print(collected)
    assert len(collected) == 3
    assert all(msg['batch'] == 'post' for msg in collected)
    assert all(msg['batch'] == 'post' for msg in collected)
    assert [msg['index'] for msg in collected] == [0, 1, 2]


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_delivery_policy_by_start_sequence():
    """Test that 'by_start_sequence' delivery policy retrieves messages from a specific sequence number."""
    subject = f'test.messenger.delivery_policy.seq.{uuid.uuid4()}'
    collected_all = []
    target_seq = None

    async with Messenger().context(host='localhost', port=4222) as mess:
        # Ensure clean stream
        await mess.purge(subject)

        # Publish 5 messages
        pub = get_publisher(subject)
        for i in range(5):
            await pub.publish(data={'index': i})
            await asyncio.sleep(0.01)

        # First read all messages to get their sequence numbers
        sub_all = get_reader(subject, deliver_policy='all')
        async for msg, metadata in sub_all:
            collected_all.append((msg, metadata))
            if msg['index'] == 2:  # Remember sequence of 3rd message
                target_seq = metadata['nats']['seq']
            if len(collected_all) >= 5:  # Stop after all messages
                break

        await sub_all.close()

        # Verify we got the target sequence
        assert target_seq is not None, "Failed to get sequence number for target message"

        # Now read using by_start_sequence from the 3rd message
        collected = []
        sub = get_reader(subject,
                         deliver_policy='by_start_sequence',
                         opt_start_seq=target_seq)

        async for msg, _ in sub:
            collected.append(msg)
            if len(collected) >= 3:  # We expect only 3 messages
                break

        await pub.close()
        await sub.close()

    # Should get messages from sequence 3 onwards (index 2, 3, 4)
    assert len(collected) == 3
    assert [msg['index'] for msg in collected] == [2, 3, 4]


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_reader_sequence_tracking():
    """Test that MsgReader properly tracks the last sequence for reconnection."""
    subject = f'test.messenger.reader_seq_tracking.{uuid.uuid4()}'
    collected_first = []
    collected_second = []

    async with Messenger().context(host='localhost', port=4222) as mess:
        # Ensure clean stream
        await mess.purge(subject)

        # Publish 5 messages
        pub = get_publisher(subject)
        for i in range(5):
            await pub.publish(data={'index': i, 'batch': 1})
            await asyncio.sleep(0.01)

        # Read first batch with normal reader (but only first 4 messages)
        sub = get_reader(subject, deliver_policy='all')
        async for msg, _ in sub:
            collected_first.append(msg)
            if msg['index'] == 3:  # Stop after 4 messages (indices 0-3)
                break

        # Force a reconnection with the reader
        await sub._reopen()

        # Publish 3 more messages
        for i in range(3):
            await pub.publish(data={'index': i, 'batch': 2})
            await asyncio.sleep(0.01)

        # Continue reading - should get remaining message from first batch + new messages
        async for msg, metadata in sub:
            print(f"Message after reconnect: index={msg['index']}, batch={msg['batch']}, seq={metadata['nats']['seq']}")
            collected_second.append(msg)
            if len(collected_second) >= 4:  # Last from first batch + 3 new ones = 4
                break

        await pub.close()
        await sub.close()

    # First batch should have first 4 messages
    assert len(collected_first) == 4
    assert [msg['index'] for msg in collected_first] == [0, 1, 2, 3]
    assert all(msg['batch'] == 1 for msg in collected_first)

    # Second batch: last message from batch 1 + all 3 from batch 2
    assert len(collected_second) == 4
    assert [msg['index'] for msg in collected_second] == [4, 0, 1, 2]
    assert collected_second[0]['batch'] == 1  # Last message from first batch
    assert all(msg['batch'] == 2 for msg in collected_second[1:])  # New messages