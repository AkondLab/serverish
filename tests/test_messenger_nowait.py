"""Test nowait functionality for MsgReader"""
import logging
import asyncio
import pytest

from serverish.messenger import Messenger, get_publisher, get_reader

log = logging.getLogger(__name__)


@pytest.mark.nats
async def test_nowait_with_messages(messenger, unique_subject):
    """Test that nowait=True returns all available messages without hanging"""
    subject = unique_subject

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

    log.info(f"Successfully read {len(received)} messages in {elapsed:.2f}s with nowait=True")


@pytest.mark.nats
async def test_nowait_eager_finish_after_batch(messenger, unique_subject):
    """nowait=True must return as soon as the server confirms end-of-data.

    ``fetch_available`` ends deterministically on the ``num_pending == 0``
    stamp of the last delivered message — no grace period, no waiting for
    the trailing 404/408 status. Prior to the fix, the reader spent the
    full 2s ``fetch_timeout`` on the trailing pull even on localhost.
    """
    subject = unique_subject

    pub = get_publisher(subject=subject)
    await messenger.purge(subject)
    for i in range(10):
        await pub.publish(data={'index': i})
    await asyncio.sleep(0.1)

    reader = get_reader(subject=subject, deliver_policy='all', nowait=True)

    received = []
    start = asyncio.get_event_loop().time()
    async for data, _meta in reader:
        received.append(data)
    elapsed = asyncio.get_event_loop().time() - start

    await reader.close()

    assert len(received) == 10
    # On localhost the whole read is a few network round trips (~ms);
    # the bound only needs to sit safely below the old ~2s failure mode
    # while tolerating CI noise.
    assert elapsed < 1.0, (
        f"nowait=True on a small batch took {elapsed:.2f}s — "
        "eager-finish regression (num_pending exit not working?)"
    )
    log.info(f"Eager finish: {len(received)} messages in {elapsed:.2f}s")


@pytest.mark.nats
async def test_nowait_exact_batch_no_closing_status(messenger, unique_subject):
    """Exact-batch worst case: server sends NO closing status at all.

    When available messages exactly fill the pull batch (reader.batch = 100),
    the server responds with the batch and neither 404 nor 408 — there is
    nothing to wait for. Before the num_pending-driven exit this path burned
    the full 2s fetch timeout per pull; now the batch-full + num_pending
    check returns immediately.
    """
    subject = unique_subject

    pub = get_publisher(subject=subject)
    await messenger.purge(subject)
    for i in range(100):  # exactly MsgReader batch size
        await pub.publish(data={'index': i})
    await asyncio.sleep(0.1)

    reader = get_reader(subject=subject, deliver_policy='all', nowait=True)

    received = []
    start = asyncio.get_event_loop().time()
    async for data, _meta in reader:
        received.append(data)
    elapsed = asyncio.get_event_loop().time() - start

    await reader.close()

    assert len(received) == 100
    assert elapsed < 1.5, (
        f"nowait=True on an exact batch took {elapsed:.2f}s — "
        "server sends no closing status here, exit must come from num_pending"
    )
    log.info(f"Exact batch: {len(received)} messages in {elapsed:.2f}s")


@pytest.mark.nats
async def test_nowait_last_per_subject_snapshot(messenger, unique_subject):
    """Snapshot pattern (tcsctl-style): last_per_subject over a subject tree.

    Verifies that num_pending is correct for filtered last_per_subject
    consumers: it must count only deliverable messages (one per subject),
    not all messages in the stream — otherwise the eager exit would fire
    too late or never.
    """
    pub_root = unique_subject

    await messenger.purge(pub_root)
    for revision in range(3):
        for k in range(5):
            pub = get_publisher(subject=f"{pub_root}.k{k}")
            await pub.publish(data={'k': k, 'revision': revision})
    await asyncio.sleep(0.1)

    reader = get_reader(subject=f"{pub_root}.>", deliver_policy='last_per_subject', nowait=True)

    received = []
    start = asyncio.get_event_loop().time()
    async for data, _meta in reader:
        received.append(data)
    elapsed = asyncio.get_event_loop().time() - start

    await reader.close()

    # one message per subject, each the latest revision
    assert len(received) == 5
    assert all(data['revision'] == 2 for data in received)
    assert elapsed < 1.0, (
        f"last_per_subject snapshot took {elapsed:.2f}s — "
        "num_pending eager exit not effective for filtered consumers?"
    )
    log.info(f"Snapshot: {len(received)} messages in {elapsed:.2f}s")


@pytest.mark.nats
async def test_nowait_empty_subject(messenger, unique_subject):
    """Test that nowait=True returns immediately when no messages exist"""
    subject = unique_subject

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

    log.info(f"Empty subject returned immediately in {elapsed:.2f}s with nowait=True")


@pytest.mark.nats
async def test_nowait_large_batch(messenger, unique_subject):
    """Test that nowait=True handles large message batches correctly"""
    subject = unique_subject

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

    log.info(f"Successfully read {len(received)} messages in {elapsed:.2f}s with nowait=True")


@pytest.mark.nats
async def test_nowait_false_waits(messenger, unique_subject):
    """Test that nowait=False waits for new messages"""
    subject = unique_subject

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

    log.info(f"nowait=False correctly waited for new message")


@pytest.mark.nats
async def test_nowait_with_deliver_policy_last(messenger, unique_subject):
    """Test nowait with deliver_policy='last'"""
    subject = unique_subject

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

    log.info(f"deliver_policy='last' with nowait=True returned only last message")
