import asyncio
import os

import nats
import nats.errors
import pytest
from nats.aio.subscription import Subscription
from nats.js.api import ConsumerConfig, DeliverPolicy

from tests.test_connection import ci
from tests.test_nats import is_nats_running


# @pytest.mark.asyncio
# @pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
# async def test_js_strem_wrong_name():
#     nc = await nats.connect("localhost")
#     js = nc.jetstream()
#     try:
#         await js.add_stream(name="stream.with.wrong.name")
#     except nats.errors.Error as e:
#         assert not isinstance(e, nats.errors.TimeoutError) , \
#             "TimeoutError should not be raised, see https://github.com/nats-io/nats.py/issues/471"
#     else:
#         pytest.fail("Error should have been raised")
#     finally:
#         await nc.close()

@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_js_strem_good_name():
    nc = await nats.connect("localhost")
    js = nc.jetstream()
    try:
        await js.add_stream(name="test-goodnametest", subjects=["fooxxx"])
    except nats.errors.Error as e:
        pytest.fail(f"Error should not have been raised on stream creation {e}")
    finally:
        await nc.close()

@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_js_seq():
    """Testing NATS JetStram seq behaviour"""
    subject = "test.natsjs.test_js_seq"
    payloads = (b'dupa1', b'dupa2', b'dupa3')
    nc = await nats.connect("localhost")
    js = nc.jetstream()
    stream = await js.find_stream_name_by_subject(subject)
    await js.purge_stream(stream, subject=subject)
    await js.publish(subject=subject, payload=payloads[0])
    await js.publish(subject=subject, payload=payloads[1])
    await js.publish(subject=subject, payload=payloads[2])


    subscription = await js.subscribe(subject, durable="my_durable")
    assert isinstance(subscription, Subscription)
    messages = []
    for p in payloads:
        msg = await subscription.next_msg(timeout=1)
        await msg.ack()
        assert msg.data == p
        messages.append(msg)

    m1 = messages[1]
    assert isinstance(m1.metadata.sequence.stream, int)
    assert m1.metadata.sequence.stream > 0

    # New subscription to read the same messages from second (index=1) using opt_start_seq
    consumer_config = ConsumerConfig(
        deliver_policy=DeliverPolicy.BY_START_SEQUENCE,
        opt_start_seq=m1.metadata.sequence.stream  # tutaj podajesz numer sekwencji
    )
    new_subscription = await js.subscribe(subject, config=consumer_config)
    new_msg = await new_subscription.next_msg(timeout=1)
    await new_msg.ack()
    assert new_msg.data == payloads[1]
    assert new_msg.metadata.sequence.stream == m1.metadata.sequence.stream

    await subscription.unsubscribe()
    await nc.close()

# @pytest.mark.asyncio
# @pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
# async def test_js_many_pub():
#     nc = await nats.connect("localhost")
#     js = nc.jetstream()
#     tasks = []
#     for i in range(100000):
#         task = asyncio.create_task(js.publish("foo", f"hello world: {i}".encode(), timeout=500))
#         tasks.append(task)
#     responses = await asyncio.gather(*tasks)