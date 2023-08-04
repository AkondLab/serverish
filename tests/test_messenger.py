import asyncio
import datetime
import logging

import pytest
import socket


from serverish.connection import Connection
from serverish.connection_jets import ConnectionJetStream
from serverish.connection_nats import ConnectionNATS
from serverish.messenger import Messenger, get_publisher, get_subscription
from tests.test_connection import ci
from tests.test_nats import is_nats_running, ensure_stram_for_tests


@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_pub_simple():
    await ensure_stram_for_tests("srvh-test", "test.messenger")

    await Messenger().open(host='localhost', port=4222)
    pub = await get_publisher('test.messenger')
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
    await ensure_stram_for_tests("srvh-test", "test.messenger")

    async with Messenger().context(host='localhost', port=4222):
        assert Messenger().is_open
    assert not Messenger().is_open




@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_pub_sub():

    now = datetime.datetime.now()
    pub = await get_publisher('test.messenger')
    sub = await get_subscription('test.messenger', deliver_policy='by_start_time', opt_start_time=now)

    async def subsciber_task(sub):
        async for msg in sub:
            print(msg)
            if msg['final']:
                break

    async def publisher_task(pub):
        for i in range(10):
            await pub.publish(data={'n': i, 'final': False})
            await asyncio.sleep(0.1)
        await pub.publish(data={'n': 10, 'final': True})

    async with Messenger().context(host='localhost', port=4222):
        await asyncio.gather(subsciber_task(sub), publisher_task(pub))
        await pub.close()
        await sub.close()


@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_pub_sub_pub():

    now = datetime.datetime.now()
    pub = await get_publisher('test.messenger')
    # sub = await get_subscription('test.messenger', deliver_policy='by_start_time', opt_start_time=now)
    sub = await get_subscription('test.messenger', deliver_policy='all')
    collected = []
    async def subsciber_task(sub):
        async for msg in sub:
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
        # subscribe and publish 11 more
        await asyncio.gather(subsciber_task(sub), publisher_task(pub, finalize=True))
        await pub.close()
        await sub.close()
    assert len(collected) == 21


async def test_messenger_pub_time_pub_sub():

    pub = await get_publisher('test.messenger')
    collected = []
    async def subsciber_task(sub):
        async for msg in sub:
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
        sub = await get_subscription('test.messenger', deliver_policy='by_start_time', opt_start_time=now)
        await subsciber_task(sub)
        await pub.close()
        await sub.close()
    assert len(collected) == 11 # only the 11 published after `now`

#
# @pytest.mark.asyncio  # This tells pytest this test is async
# @pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
# async def test_messenger_cm():
#
#     async def subsciber_task(sub):
#         async for msg in sub:
#             print(msg.data)
#             if msg.data['final']:
#                 break
#
#     async def publisher_task(pub):
#         for i in range(10):
#             await pub.publish(data={'n': i, 'final': False})
#             await asyncio.sleep(0.1)
#         await pub.publish(data={'n': 10, 'final': True})
#
#     async with get_publisher('test.messenger') as pub, get_subscriber('test.messenger', deliver_policy='new') as sub:
#         await asyncio.gather(subsciber_task(sub), publisher_task(pub))
#
# @pytest.mark.asyncio  # This tells pytest this test is async
# @pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
# async def test_progrssor_cm2():
#
#     prog = await get_messenger('test.messenger')
#     sub = await get_subscriber('test.messenger', deliver_policy='new')
#
#
