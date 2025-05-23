import asyncio
import datetime
import logging
from asyncio import Lock

import pytest

from serverish.base import Task, create_task
from serverish.messenger import Messenger, get_publisher, get_reader
from tests.test_connection import ci
from tests.test_nats import is_nats_running


@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_pub_simple():
    # await ensure_stram_for_tests("srvh-test", "test.messenger.test_messenger_pub_simple")

    await Messenger().open(host='localhost', port=4222)
    pub = get_publisher('test.messenger.test_messenger_pub_simple')
    await pub.publish(data={'msg': 'test_messenger_pub'},
                      meta={
                          'sender': 'test_messenger_pub',
                          'trace_level': logging.WARN,
                      })
    await Messenger().close()


@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_pub_simple_cm():
    async with Messenger().context(host='localhost', port=4222):
        assert Messenger().is_open
    assert not Messenger().is_open


@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_pub3_then_sub():

    subject = 'test.messenger.test_messenger_pub3_then_sub'
    lock = Lock()

    async def subsciber_task(sub):
        async for data, meta in sub:
            async with lock:
                print(data)
            if data['final']:
                break

    async def publisher_task(pub, n):
        for i in range(n):
            await pub.publish(data={'n': i, 'final': False})
            await asyncio.sleep(0.01)

    async def publish_final(pub):
        await pub.publish(data={'n': 9999, 'final': True})

    async with Messenger().context(host='localhost', port=4222) as mes:
        await mes.purge(subject)
        pub = get_publisher(subject=subject)
        sub = get_reader(subject=subject, deliver_policy='all')

        await publisher_task(pub, 3)

        t = await create_task(subsciber_task(sub), "sub")

        logging.info('subscriber started')
        await asyncio.sleep(0.03)
        logging.info('2nd publisher starting')
        await publisher_task(pub, 2)


        await asyncio.sleep(3)
        await publish_final(pub)

        await t
        await pub.close()
        await sub.close()


@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_pub_sub():

    subject = 'test.messenger.messenger_pub_sub'

    now = datetime.datetime.now() - datetime.timedelta(minutes=5)

    async def subsciber_task(sub):
        async for data, meta in sub:
            print(data)
            if data['final']:
                break

    async def publisher_task(pub):
        for i in range(10):
            await pub.publish(data={'n': i, 'final': False})
            await asyncio.sleep(0.1)
        await pub.publish(data={'n': 10, 'final': True})

    async with Messenger().context(host='localhost', port=4222) as mes:
        await mes.purge(subject)
        pub = get_publisher(subject=subject)
        sub = get_reader(subject=subject, deliver_policy='all')
        # sub = get_reader(subject=subject, deliver_policy='by_start_time', opt_start_time=now)
        await asyncio.gather(subsciber_task(sub), publisher_task(pub))
        await pub.close()
        await sub.close()

@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_pub_then_sub():
    subject = 'test.messenger.test_messenger_pub_then_sub'

    now = datetime.datetime.now()
    pub = get_publisher(subject)
    sub = get_reader(subject,
                     deliver_policy='all',
                     # deliver_policy='by_start_time',
                     # opt_start_time=now,
                     )

    async def subsciber_task(sub):
        async for data, meta in sub:
            print(data)
            if data['final']:
                break

    async def publisher_task(pub):
        meta = {'trace_level': logging.WARN}

        for i in range(10):
            await pub.publish(data={'n': i, 'final': False}, meta=meta)
            await asyncio.sleep(0.1)
        await pub.publish(data={'n': 10, 'final': True}, meta=meta)

    async with Messenger().context(host='localhost', port=4222):
        await Messenger().purge(subject)
        await publisher_task(pub)
        await asyncio.sleep(0.1)
        await subsciber_task(sub)
        await pub.close()
        await sub.close()




@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_pub_sub_pub():

    now = datetime.datetime.now()
    collected = []
    async def subsciber_task(sub):
        async for msg, meta in sub:
            print(msg)
            collected.append(msg)
            if msg['final']:
                break

    async def publisher_task(pub, finalize=False):
        meta = {'trace_level': logging.WARN}
        await asyncio.sleep(0.1)
        for i in range(10):
            await pub.publish(data={'n': i, 'final': False}, meta=meta)
            await asyncio.sleep(0.1)
        if finalize:
            await pub.publish(data={'n': 10, 'final': True}, meta=meta)

    async with Messenger().context(host='localhost', port=4222) as mess:
        await mess.purge('test.messenger.test_messenger_pub_sub_pub')
        pub = get_publisher('test.messenger.test_messenger_pub_sub_pub')
        sub = get_reader('test.messenger.test_messenger_pub_sub_pub', deliver_policy='all')

        await publisher_task(pub, finalize=False) # pre-publish 10
        await asyncio.sleep(0.1)
        # subscribe and publish 11 more
        await asyncio.gather(subsciber_task(sub), publisher_task(pub, finalize=True))
        await pub.close()
        await sub.close()
    assert len(collected) == 21

@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_big_pub_small_sub():
    """Test reading a larger number of messages with a smaller batch size."""
    subject = f'test.messenger.big_pub_small_sub'
    total_messages = 15
    batch_size = 10
    collected = []

    async with Messenger().context(host='localhost', port=4222) as mess:
        # Clean up from previous runs
        await mess.purge(subject)

        # Publish 15 messages
        pub = get_publisher(subject)
        for i in range(total_messages):
            await pub.publish(data={'index': i})
            await asyncio.sleep(0.0)

        # Read with smaller batch size (10)
        sub = get_reader(subject, deliver_policy='all')
        assert sub.batch == 100
        sub.batch = 10
        assert sub.batch == 10


        # Start timing to verify it doesn't get stuck
        start_time = datetime.datetime.now()

        # Read all messages in one loop
        async for msg, meta in sub:
            collected.append(msg)
            if len(collected) >= total_messages:  # Stop after reading all messages
                break

        # Check timing
        elapsed_time = (datetime.datetime.now() - start_time).total_seconds()

        await pub.close()
        await sub.close()

    # Verify we got all messages
    assert len(collected) == total_messages
    assert [msg['index'] for msg in collected] == list(range(total_messages))

    # Verify it didn't take too long (should be quick)
    assert elapsed_time < 1.0, f"Reading took too long: {elapsed_time} seconds"

@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_immediate_message_delivery():
    """Test that reader returns messages immediately when they appear, not waiting for batch to fill."""
    subject = f'test.messenger.immediate_delivery.{datetime.datetime.now().timestamp()}'
    initial_messages = 5
    total_messages = 10
    received_timestamps = []
    publish_timestamps = []
    # batch_size = 10  # Set batch size larger than initial messages

    async def slow_publisher(pub):
        """Publishes remaining messages with deliberate delay"""
        await asyncio.sleep(0.5)  # Initial delay before publishing more

        for i in range(initial_messages, total_messages):
            time_before_publish = datetime.datetime.now()
            await pub.publish(data={'index': i})
            publish_timestamps.append((i, time_before_publish))
            await asyncio.sleep(0.3)  # Slow publishing rate

    async def reader_task(sub):
        """Reads messages and records when they were received"""
        async for msg, _ in sub:
            received_time = datetime.datetime.now()
            index = msg['index']
            received_timestamps.append((index, received_time))

            if len(received_timestamps) >= total_messages:
                break

    async with Messenger().context(host='localhost', port=4222) as mess:
        # Clean up from previous runs
        await mess.purge(subject)

        # Publish initial batch of messages
        pub = get_publisher(subject)
        for i in range(initial_messages):
            time_before_publish = datetime.datetime.now()
            await pub.publish(data={'index': i})
            publish_timestamps.append((i, time_before_publish))
            await asyncio.sleep(0.01)

        # Set up reader with custom batch size
        sub = get_reader(subject, deliver_policy='all')
        # sub.batch = batch_size  # Intentionally larger than initial_messages

        # Start both tasks
        reader_future = asyncio.create_task(reader_task(sub))
        publisher_future = asyncio.create_task(slow_publisher(pub))

        # Wait for both to complete
        await asyncio.gather(reader_future, publisher_future)

        await pub.close()
        await sub.close()

    # Verify all messages were received
    assert len(received_timestamps) == total_messages

    # Calculate delivery delays
    delivery_delays = {}
    for index, recv_time in received_timestamps:
        # Find matching publish time
        matching_publish = next((t for i, t in publish_timestamps if i == index), None)
        if matching_publish:
            delay = (recv_time - matching_publish).total_seconds()
            delivery_delays[index] = delay

    # Check delays for messages published after reader started
    # These should be delivered quickly, not waiting for batch to fill
    for i in range(initial_messages, total_messages):
        assert delivery_delays[i] < 0.2, f"Message {i} delivery delay was {delivery_delays[i]:.3f}s"

    # Print summary of results
    indices = [msg[0] for msg in received_timestamps]
    assert indices == list(range(total_messages)), f"Messages received out of order: {indices}"



@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skip("Experimental long test, not for automated testing")
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_pub_time_pub_sub():

    pub = get_publisher('test.messenger.test_messenger_pub_time_pub_sub')
    collected = []
    async def subsciber_task(sub):
        async for msg, meta in sub:
            print(msg)
            collected.append(msg)
            if msg['final']:
                break

    async def publisher_task(pub, finalize=False):
        await asyncio.sleep(0.1)
        for i in range(10):
            await pub.publish(data={'n': i, 'final': False})
            await asyncio.sleep(0.1)
        if finalize:
            await pub.publish(data={'n': 10, 'final': True})

    async with Messenger().context(host='localhost', port=4222):
        await publisher_task(pub, finalize=False) # pre-publish 10
        await asyncio.sleep(0.1)
        now = datetime.datetime.now()
        await publisher_task(pub, finalize=False) # publish 11 more
        sub = get_reader('test.messenger.test_messenger_pub_time_pub_sub', deliver_policy='by_start_time', opt_start_time=now)
        await subsciber_task(sub)
        await pub.close()
        await sub.close()
    assert len(collected) == 11 # only the 11 published after `now`


@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_scheduled_open():
    """Test that messenger will open using scheduled_open, wait for beeing open, checks if is open then  close itself"""
    msg = Messenger()
    t = await msg.open(host='localhost', port=4222, wait=False)
    assert not msg.is_open
    await t.task
    assert msg.is_open
    await msg.close()
    assert not msg.is_open

@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_scheduled_open_fail():
    """Test that messenger will open using scheduled_open, wait for beeing open, checks if is open then  close itself"""
    msg = Messenger()
    t = await msg.open(host='localhost', port=4225, wait=False)
    assert not msg.is_open
    with pytest.raises(TimeoutError):
        await t.wait_for(0.1)
    assert not msg.is_open
    await msg.close()
    assert not msg.is_open




