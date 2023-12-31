import asyncio

import pytest
import socket

from serverish.connection.connection_jets import ConnectionJetStream
from serverish.connection.connection_nats import ConnectionNATS
from serverish.base.status import StatusEnum
from tests.test_connection import ci


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

async def ensure_stram_for_tests(stream, subject):
    c = ConnectionJetStream(host='localhost', port=4222)
    async with c:
        await c.ensure_subject_in_stream(stream, subject, create_stram_if_needed=True)



@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
@pytest.mark.skipif(ci, reason="Not working on CI")
async def test_nats():
    c = ConnectionNATS(host='localhost', port=4222)
    async with c:
        codes = await c.diagnose(no_deduce=True)
        for s in codes.values():
            assert s == StatusEnum.ok

@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
@pytest.mark.skipif(ci, reason="Not working on CI")
async def test_jests():
    c = ConnectionJetStream(host='localhost', port=4222, streams={'test': {}})
    async with c:
        codes = await c.diagnose(no_deduce=True)
        for s in codes.values():
            assert s in [StatusEnum.ok, StatusEnum.na]

@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
@pytest.mark.skipif(ci, reason="Not working on CI")
@pytest.mark.xfail(reason="This test is expected to fail on dot in stream name")
async def test_jests_wrongname():
    c = ConnectionJetStream(host='localhost', port=4222, streams={'test.foo': {}})
    async with c:
        codes = await c.diagnose(no_deduce=True)
        for s in codes.values():
            assert s == 'ok'

@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_nats_publish():
    message_received = asyncio.Event()
    received_messages = []

    async def message_handler(msg):
        received_messages.append(msg.data.decode())
        message_received.set()

    c = ConnectionNATS(host='localhost', port=4222)
    async with c:
        await c.nc.subscribe("test.js.test_nats_publish", cb=message_handler)
        await c.nc.publish('test.js.test_nats_publish', b'Hello OCA!')
        try:
            await asyncio.wait_for(message_received.wait(), timeout=1)
        except asyncio.TimeoutError:
            pytest.fail("Timeout exceeded while waiting for message")
        assert len(received_messages) == 1
        assert received_messages[0] == "Hello OCA!"

@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_js_publish_subscribe():
    # await ensure_stram_for_tests('test', 'test.js.foo1')

    message_received = asyncio.Event()
    received_messages = []

    async def message_handler(msg):
        received_messages.append(msg.data.decode())
        message_received.set()

    c = ConnectionJetStream(host='localhost', port=4222)
    async with c:
        await c.js.publish('test.js.test_js_publish_subscribe', b'Hello OCA!')
        await c.js.subscribe("test.js.test_js_publish_subscribe", cb=message_handler, deliver_policy='last')
        try:
            await asyncio.wait_for(message_received.wait(), timeout=1)
        except asyncio.TimeoutError:
            pytest.fail("Timeout exceeded while waiting for message")
        assert len(received_messages) == 1
        assert received_messages[0] == "Hello OCA!"


@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_js_subscribe_publish():
    # await ensure_stram_for_tests('srvh-test', 'test.js.foo1')

    message_received = asyncio.Event()
    received_messages = []

    async def message_handler(msg):
        received_messages.append(msg.data.decode())
        message_received.set()

    c = ConnectionJetStream(host='localhost', port=4222)
    async with c:
        await c.js.subscribe("test.js.test_js_subscribe_publish", cb=message_handler, deliver_policy='new')
        await c.js.publish('test.js.test_js_subscribe_publish', b'Hello OCA!')
        try:
            await asyncio.wait_for(message_received.wait(), timeout=1)
        except asyncio.TimeoutError:
            pytest.fail("Timeout exceeded while waiting for message")
        assert len(received_messages) == 1
        assert received_messages[0] == "Hello OCA!"


