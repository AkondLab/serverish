import asyncio
import datetime
import logging
import uuid

import pytest

from serverish.messenger import Messenger, get_publisher, get_reader
from serverish.messenger.msg_reader import MsgReader
from serverish.base.exceptions import MessengerReaderConfigError


@pytest.mark.nats
async def test_delivery_policy_all(messenger, unique_subject):
    """Test that 'all' delivery policy retrieves all messages from the stream."""
    subject = unique_subject
    collected = []

    # Ensure clean stream
    await messenger.purge(subject)

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


@pytest.mark.nats
async def test_delivery_policy_last(messenger, unique_subject):
    """Test that 'last' delivery policy retrieves only the last message from the stream."""
    subject = unique_subject
    collected = []

    # Ensure clean stream
    await messenger.purge(subject)

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


@pytest.mark.nats
async def test_delivery_policy_new(messenger, unique_subject):
    """Test that 'new' delivery policy retrieves only new messages published after subscription."""
    subject = unique_subject
    collected = []

    # Ensure clean stream
    await messenger.purge(subject)

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


@pytest.mark.nats
async def test_delivery_policy_by_start_time(messenger, unique_subject):
    """Test that 'by_start_time' delivery policy retrieves messages published after specific time."""
    subject = unique_subject
    collected = []

    # Ensure clean stream
    await messenger.purge(subject)

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


@pytest.mark.nats
async def test_delivery_policy_by_start_sequence(messenger, unique_subject):
    """Test that 'by_start_sequence' delivery policy retrieves messages from a specific sequence number."""
    subject = unique_subject
    collected_all = []
    target_seq = None

    # Ensure clean stream
    await messenger.purge(subject)

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


@pytest.mark.nats
async def test_reader_sequence_tracking(messenger, unique_subject):
    """Test that MsgReader properly tracks the last sequence for reconnection."""
    subject = unique_subject
    collected_first = []
    collected_second = []

    # Ensure clean stream
    await messenger.purge(subject)

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


# ---------------------------------------------------------------------------
# Unit tests for zero-microsecond opt_start_time workaround (no NATS needed)
# ---------------------------------------------------------------------------

async def test_consumer_cfg_zero_microsecond_is_bumped_to_one():
    """opt_start_time with microsecond==0 must be bumped to 1 µs.

    nats-py's Base._to_utc_iso strips '.000000' on whole-second values, but
    Base._parse_utc_iso unconditionally splits on '.' — so an echo of the
    stored timestamp raises ValueError.  The workaround pads zero-microsecond
    timestamps to 1 µs before sending to the server.
    """
    m = Messenger()
    t0 = datetime.datetime(2026, 4, 25, 16, 0, 0, 0, tzinfo=datetime.timezone.utc)
    assert t0.microsecond == 0

    rdr = MsgReader('test.unit', parent=m, deliver_policy='by_start_time',
                    opt_start_time=t0)
    cfg = await rdr._create_consumer_cfg()

    # The formatted string must contain a fractional part (not end in ':00Z')
    assert cfg.opt_start_time is not None
    assert '.' in cfg.opt_start_time, (
        f"opt_start_time '{cfg.opt_start_time}' has no fractional part; "
        "nats-py _parse_utc_iso would raise ValueError"
    )
    assert not cfg.opt_start_time.endswith('.000000Z'), (
        "opt_start_time still has zero microseconds — nats-py round-trip will fail"
    )


async def test_consumer_cfg_nonzero_microsecond_is_unchanged():
    """opt_start_time with microsecond != 0 must not be modified."""
    m = Messenger()
    t = datetime.datetime(2026, 4, 25, 16, 0, 0, 123456, tzinfo=datetime.timezone.utc)
    rdr = MsgReader('test.unit', parent=m, deliver_policy='by_start_time',
                    opt_start_time=t)
    cfg = await rdr._create_consumer_cfg()

    assert cfg.opt_start_time == '2026-04-25T16:00:00.123456Z'


# ---------------------------------------------------------------------------
# Unit tests for deliver_policy validation (no NATS needed)
# ---------------------------------------------------------------------------

def test_by_start_time_without_opt_start_time_raises():
    """MsgReader must raise ValueError at construction when deliver_policy='by_start_time'
    but opt_start_time is not provided."""
    m = Messenger()
    with pytest.raises(ValueError, match="opt_start_time"):
        MsgReader('test.unit', parent=m, deliver_policy='by_start_time')


def test_by_start_time_with_opt_start_time_succeeds():
    """MsgReader must succeed when deliver_policy='by_start_time' and opt_start_time is set."""
    m = Messenger()
    t = datetime.datetime(2026, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    rdr = MsgReader('test.unit', parent=m, deliver_policy='by_start_time', opt_start_time=t)
    assert rdr.deliver_policy == 'by_start_time'


def test_by_start_sequence_without_opt_start_seq_raises():
    """MsgReader must raise ValueError at construction when deliver_policy='by_start_sequence'
    but opt_start_seq is not provided."""
    m = Messenger()
    with pytest.raises(ValueError, match="opt_start_seq"):
        MsgReader('test.unit', parent=m, deliver_policy='by_start_sequence')


def test_by_start_sequence_with_opt_start_seq_succeeds():
    """MsgReader must succeed when deliver_policy='by_start_sequence' and opt_start_seq is set."""
    m = Messenger()
    rdr = MsgReader('test.unit', parent=m, deliver_policy='by_start_sequence',
                    consumer_cfg={'opt_start_seq': 42})
    assert rdr.deliver_policy == 'by_start_sequence'


def test_messenger_get_reader_by_start_time_without_time_raises():
    """Messenger.get_reader must raise ValueError when by_start_time is used without opt_start_time."""
    with pytest.raises(ValueError, match="opt_start_time"):
        Messenger.get_reader('test.unit', deliver_policy='by_start_time')


# ---------------------------------------------------------------------------
# Unit test: MessengerReaderConfigError is exported from serverish.base
# ---------------------------------------------------------------------------

def test_messenger_reader_config_error_is_exported():
    """MessengerReaderConfigError must be importable from serverish.base."""
    from serverish.base import MessengerReaderConfigError as Err  # noqa: F401
    assert issubclass(Err, ValueError)


# ---------------------------------------------------------------------------
# Unit test: fatal NATS errors (BadRequestError / NotFoundError) raise
# MessengerReaderConfigError from __anext__ (no live NATS needed)
# ---------------------------------------------------------------------------

async def test_fatal_nats_bad_request_raises_config_error():
    """When open() raises nats.js.errors.BadRequestError the read loop must
    stop and raise MessengerReaderConfigError instead of retrying forever."""
    import nats.js.errors

    m = Messenger()
    t = datetime.datetime(2026, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    rdr = MsgReader('test.unit', parent=m, deliver_policy='by_start_time',
                    opt_start_time=t, error_behavior='WAIT')

    # Patch open() so it raises BadRequestError immediately
    async def _bad_open():
        raise nats.js.errors.BadRequestError()

    rdr.open = _bad_open

    with pytest.raises(MessengerReaderConfigError):
        async for _ in rdr:
            pass  # pragma: no cover


async def test_fatal_nats_not_found_raises_config_error():
    """When open() raises nats.js.errors.NotFoundError the read loop must
    stop and raise MessengerReaderConfigError."""
    import nats.js.errors

    m = Messenger()
    rdr = MsgReader('test.unit', parent=m, deliver_policy='all',
                    error_behavior='WAIT')

    async def _not_found_open():
        raise nats.js.errors.NotFoundError()

    rdr.open = _not_found_open

    with pytest.raises(MessengerReaderConfigError):
        async for _ in rdr:
            pass  # pragma: no cover

