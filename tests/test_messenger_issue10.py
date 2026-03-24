import time

import nats
import pytest

from serverish.messenger import Messenger, get_reader


@pytest.mark.nats
async def test_pull_subscribe_long_cpu_bound(nats_server, unique_subject):
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
        servers=[f'nats://{nats_server["host"]}:{nats_server["port"]}'],
        error_cb=error_handler,
        disconnected_cb=disconnected_handler,
        closed_cb=closed_handler,
        discovered_server_cb=discovered_server_handler,
        reconnected_cb=reconnected_handler,
    )

    js = nc.jetstream()
    stream = await js.find_stream_name_by_subject(unique_subject)
    await js.purge_stream(stream)

    for i in range(2):
        ack = await js.publish(unique_subject, f"{i}".encode())

    consumer = await js.pull_subscribe(unique_subject, "dur")

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
