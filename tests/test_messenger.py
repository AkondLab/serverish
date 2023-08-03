import asyncio
import datetime
import logging

import pytest
import socket


from serverish.connection import Connection
from serverish.connection_jets import ConnectionJetStream
from serverish.connection_nats import ConnectionNATS
from serverish.messenger import get_publisher, Messenger


def is_nats_running(host='localhost', port=4222):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((host, port))
        s.shutdown(socket.SHUT_RDWR)
        return True
    except ConnectionRefusedError:
        return False
    finally:
        s.close()

@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_pub_simple():

    await Messenger().open(host='localhost', port=4222)
    pub = await get_publisher('test.messenger')
    await pub.publish(data={'msg': 'test_messenger_pub'},
                      meta={
                          'sender': 'test_messenger_pub',
                          'trace_level': logging.WARN,
                      })
    await Messenger().close()




@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_pub_sub():

    now = datetime.datetime.now()
    pub = await get_publisher('test.messenger')
    sub = await get_subscriber('test.messenger', deliver_policy='by_start_time', opt_start_time=now)

    async def subsciber_task(sub):
        async for msg in sub:
            print(msg.data)
            if msg.data['final']:
                break

    async def publisher_task(pub):
        for i in range(10):
            await pub.publish(data={'n': i, 'final': False})
            await asyncio.sleep(0.1)
        await pub.publish(data={'n': 10, 'final': True})

    await asyncio.gather(subsciber_task(sub), publisher_task(pub))
    await pub.close()
    await sub.close()

@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_cm():

    async def subsciber_task(sub):
        async for msg in sub:
            print(msg.data)
            if msg.data['final']:
                break

    async def publisher_task(pub):
        for i in range(10):
            await pub.publish(data={'n': i, 'final': False})
            await asyncio.sleep(0.1)
        await pub.publish(data={'n': 10, 'final': True})

    async with get_publisher('test.messenger') as pub, get_subscriber('test.messenger', deliver_policy='new') as sub:
        await asyncio.gather(subsciber_task(sub), publisher_task(pub))

@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_progrssor_cm2():

    prog = await get_messenger('test.messenger')
    sub = await get_subscriber('test.messenger', deliver_policy='new')


