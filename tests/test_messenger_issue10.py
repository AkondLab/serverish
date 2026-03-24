import time

import nats
import pytest

from serverish.messenger import  Messenger, get_reader
from tests.test_connection import ci
from tests.test_nats import is_nats_running


@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_pull_subscribe_long_cpu_bound():
    subject = 'test.nats.test_pull_subscribe_long_cpu_bound'
    simulate_cpu_time = 1

    nc = nats.NATS()

    async def error_handler(e: Exception):
        print("Error", e)

    async def disconnected_handler():
        print("Disconnected")

    async def closed_handler():
        print("Closed")

    async def discovered_server_handler():
        print("Discovered server")

    async def reconnected_handler():
        print("Reconnected")

    await nc.connect(
        error_cb=error_handler,
        disconnected_cb=disconnected_handler,
        closed_cb=closed_handler,
        discovered_server_cb=discovered_server_handler,
        reconnected_cb=reconnected_handler,
    )

    js = nc.jetstream()
    stream = await js.find_stream_name_by_subject(subject)
    await js.purge_stream(stream)

    # await js.add_stream(name="TEST1", subjects=["foo.1", "bar"])

    for i in range(2):
        ack = await js.publish(subject, f"{i}".encode())

    consumer = await js.pull_subscribe(subject, "dur")

    msg, *_ = await consumer.fetch(1, timeout=5)
    await msg.ack()
    print(f'Got message {msg.data}')

    time.sleep(simulate_cpu_time)
    print(f"Done CPU bound {simulate_cpu_time} seconds")

    try:
        msg, *_ = await consumer.fetch(1, timeout=10)
        await msg.ack()
        print(f'Got message {msg.data}')
    except TimeoutError:
        print("TimeoutError")

    await nc.close()

