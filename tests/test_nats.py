import asyncio
import logging

import pytest

from serverish.connection.connection_jets import ConnectionJetStream
from serverish.connection.connection_nats import ConnectionNATS
from serverish.base.status import StatusEnum


@pytest.mark.nats
async def test_nats_on_localhost(nats_server):
    c = ConnectionNATS(host=nats_server['host'], port=nats_server['port'])
    try:
        await c.connect()
        assert c.nc.is_connected
    finally:
        await c.disconnect()



@pytest.mark.nats
async def test_nats_fixture(nats_server):
    assert nats_server['host'] is not None
    assert nats_server['port'] is not None

@pytest.mark.nats
async def test_nats_server(nats_server):
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((nats_server['host'], nats_server['port']))
        s.shutdown(socket.SHUT_RDWR)
        reachable = True
    except ConnectionRefusedError:
        reachable = False
    finally:
        s.close()
    assert reachable


@pytest.mark.nats
@pytest.mark.timeout(20)
async def test_nats(nats_server):
    logging.info(f"Connecting to {nats_server['host']}:{nats_server['port']}")
    c = ConnectionNATS(host=nats_server['host'], port=nats_server['port'])
    logging.info(f"Connection gained")
    async with c:
        codes = await c.diagnose(no_deduce=True)
        for s in codes.values():
            assert s == StatusEnum.ok

@pytest.mark.nats
async def test_jests(nats_server):
    c = ConnectionJetStream(host=nats_server['host'], port=nats_server['port'], streams={'test': {}})
    async with c:
        codes = await c.diagnose(no_deduce=True)
        for s in codes.values():
            assert s in [StatusEnum.ok, StatusEnum.na]

@pytest.mark.nats
@pytest.mark.xfail(reason="This test is expected to fail on dot in stream name")
async def test_jests_wrongname(nats_server):
    c = ConnectionJetStream(host=nats_server['host'], port=nats_server['port'], streams={'test.foo': {}})
    async with c:
        codes = await c.diagnose(no_deduce=True)
        for s in codes.values():
            assert s == 'ok'

@pytest.mark.nats
async def test_nats_publish(nats_server):
    message_received = asyncio.Event()
    received_messages = []

    async def message_handler(msg):
        received_messages.append(msg.data.decode())
        message_received.set()

    c = ConnectionNATS(host=nats_server['host'], port=nats_server['port'])
    async with c:
        await c.nc.subscribe("test.js.test_nats_publish", cb=message_handler)
        await c.nc.publish('test.js.test_nats_publish', b'Hello OCA!')
        try:
            await asyncio.wait_for(message_received.wait(), timeout=1)
        except asyncio.TimeoutError:
            pytest.fail("Timeout exceeded while waiting for message")
        assert len(received_messages) == 1
        assert received_messages[0] == "Hello OCA!"

@pytest.mark.nats
async def test_js_publish_subscribe(nats_server):
    # await ensure_stram_for_tests('test', 'test.js.foo1')

    message_received = asyncio.Event()
    received_messages = []

    async def message_handler(msg):
        received_messages.append(msg.data.decode())
        message_received.set()

    c = ConnectionJetStream(host=nats_server['host'], port=nats_server['port'])
    async with c:
        await c.js.publish('test.js.test_js_publish_subscribe', b'Hello OCA!')
        await c.js.subscribe("test.js.test_js_publish_subscribe", cb=message_handler, deliver_policy='last')
        try:
            await asyncio.wait_for(message_received.wait(), timeout=1)
        except asyncio.TimeoutError:
            pytest.fail("Timeout exceeded while waiting for message")
        assert len(received_messages) == 1
        assert received_messages[0] == "Hello OCA!"


@pytest.mark.nats
async def test_js_subscribe_publish(nats_server):
    # await ensure_stram_for_tests('srvh-test', 'test.js.foo1')

    message_received = asyncio.Event()
    received_messages = []

    async def message_handler(msg):
        received_messages.append(msg.data.decode())
        message_received.set()

    c = ConnectionJetStream(host=nats_server['host'], port=nats_server['port'])
    async with c:
        await c.js.subscribe("test.js.test_js_subscribe_publish", cb=message_handler, deliver_policy='new')
        await c.js.publish('test.js.test_js_subscribe_publish', b'Hello OCA!')
        try:
            await asyncio.wait_for(message_received.wait(), timeout=1)
        except asyncio.TimeoutError:
            pytest.fail("Timeout exceeded while waiting for message")
        assert len(received_messages) == 1
        assert received_messages[0] == "Hello OCA!"
