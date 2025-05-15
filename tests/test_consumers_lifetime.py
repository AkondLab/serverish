import asyncio
import datetime
import nats
import pytest

from tests.test_connection import ci
from tests.test_nats import is_nats_running

@pytest.mark.skip("Experimental long test, not for automated testing")
@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_raw_ephemeral_consumer_expiration():
    subject = 'test.raw.consumer_expiration'
    expiration_times = []

    # Connect directly to NATS
    nc = await nats.connect("nats:4222")
    js = nc.jetstream()


    # Publish a message to have something to consume
    await js.publish(subject, b'test data')

    # Test different waiting periods
    for wait_time in [1, 2, 3, 5, 8, 10]:
        print(f"\n--- Testing with {wait_time} second wait ---")

        # Create ephemeral pull subscription (without durable name)
        pull_sub = await js.pull_subscribe(subject)

        # Initialize the consumer with a fetch
        try:
            msgs = await pull_sub.fetch(1, timeout=1)
            if msgs:
                print(f"Initial fetch successful, got {len(msgs)} message(s)")
        except Exception as e:
            print(f"Initial fetch failed: {e}")

        # Record consumer info and start time
        try:
            consumer_info = await pull_sub.consumer_info()
            start_time = datetime.datetime.now()
            print(f"Created consumer {consumer_info.name} at {start_time}")

            # Wait for specified time
            print(f"Waiting for {wait_time} seconds...")
            await asyncio.sleep(wait_time)

            # Check if consumer still exists
            try:
                info = await pull_sub.consumer_info()
                print(f"Consumer still alive after {wait_time} seconds: {info.name}")
            except nats.js.errors.NotFoundError:
                elapsed = (datetime.datetime.now() - start_time).total_seconds()
                print(f"Consumer expired after approximately {elapsed:.2f} seconds")
                expiration_times.append(elapsed)

        except Exception as e:
            print(f"Error during test: {e}")

        # Clean up
        try:
            await pull_sub.unsubscribe()
        except:
            pass

    # Report findings
    if expiration_times:
        avg_expiration = sum(expiration_times) / len(expiration_times)
        print(f"\nAverage expiration time: {avg_expiration:.2f} seconds")
        print(f"Expiration times: {expiration_times}")
    else:
        print("\nNo consumers expired during testing")

    # Clean up
    await nc.close()

@pytest.mark.skip("Experimental long test, not for automated testing")
@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_ephemeral_consumer_with_explicit_timeout():
    subject = 'test.raw.consumer_explicit_timeout'

    # Connect directly to NATS
    nc = await nats.connect("nats:4222")
    js = nc.jetstream()

    # Publish a message to have something to consume
    await js.publish(subject, b'test data')

    # Set explicit inactive_threshold (in seconds)
    inactive_threshold = 30

    print(f"\nTesting with explicit inactive_threshold={inactive_threshold}s")

    # Create consumer with explicit config
    consumer_config = nats.js.api.ConsumerConfig(
        inactive_threshold=inactive_threshold,
    )

    pull_sub = await js.pull_subscribe(subject, config=consumer_config)

    # Initialize with fetch
    try:
        msgs = await pull_sub.fetch(1, timeout=1)
        if msgs:
            print(f"Initial fetch successful, got {len(msgs)} message(s)")
    except Exception as e:
        print(f"Initial fetch failed: {e}")

    # Record consumer info and start time
    try:
        consumer_info = await pull_sub.consumer_info()
        start_time = datetime.datetime.now()
        print(f"Created consumer {consumer_info.name} at {start_time}")

        # Check if inactive_threshold was properly set
        print(f"Consumer config: inactive_threshold={consumer_info.config.inactive_threshold}s")

        # Check at intervals
        check_times = [5, 10, 15, 20, 25, 35, 40]
        for wait_time in check_times:
            await asyncio.sleep(wait_time - (datetime.datetime.now() - start_time).total_seconds())
            try:
                info = await pull_sub.consumer_info()
                elapsed = (datetime.datetime.now() - start_time).total_seconds()
                print(f"After {elapsed:.2f}s: Consumer still alive")
            except nats.js.errors.NotFoundError:
                elapsed = (datetime.datetime.now() - start_time).total_seconds()
                print(f"After {elapsed:.2f}s: Consumer expired")
                break

    except Exception as e:
        print(f"Error during test: {e}")

    # Clean up
    await nc.close()

@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_list_all_consumers():
    nc = await nats.connect("localhost:4222")
    js = nc.jetstream()

    streams = await js.streams_info()

    total_consumers = 0
    for stream in streams:
        consumers = await js.consumers_info(stream.config.name)
        stream_consumers = len(consumers)
        total_consumers += stream_consumers
        print(f"Stream {stream.config.name}: {stream_consumers} consumers")

    print(f"Total consumers across all streams: {total_consumers}")
    await nc.close()