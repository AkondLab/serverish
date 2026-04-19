"""Correctness tests for core NATS pub/sub helpers (no JetStream).

Covers MsgCorePub, MsgCoreSub, MsgCoreReader (async iteration and read_next)
plus the command-layer wrappers MsgCommandPublisher / MsgCommandSubscriber.

Core NATS subjects live under 'test_no_js.' to stay outside the session
'test.>' JetStream stream, so messages are fire-and-forget as intended.
"""
from __future__ import annotations

import asyncio

import pytest

from serverish.messenger import (
    MsgCommandPublisher,
    MsgCommandSubscriber,
    MsgCorePub,
    MsgCoreReader,
    MsgCoreSub,
    get_commandpublisher,
    get_commandsubscriber,
    get_corepublisher,
    get_corereader,
    get_coresubscriber,
)


# ---------------------------------------------------------------------------
# Factory / type smoke tests
# ---------------------------------------------------------------------------

@pytest.mark.nats
def test_get_corepublisher_returns_instance(messenger):
    pub = get_corepublisher('test_no_js.factory')
    assert isinstance(pub, MsgCorePub)


@pytest.mark.nats
def test_get_corereader_returns_instance(messenger):
    reader = get_corereader('test_no_js.factory')
    assert isinstance(reader, MsgCoreReader)


@pytest.mark.nats
def test_coresubscriber_is_corereader(messenger):
    sub = get_coresubscriber('test_no_js.factory')
    assert isinstance(sub, MsgCoreSub)
    assert isinstance(sub, MsgCoreReader)


@pytest.mark.nats
def test_commandpublisher_is_corepub(messenger):
    pub = get_commandpublisher('test_no_js.factory')
    assert isinstance(pub, MsgCommandPublisher)
    assert isinstance(pub, MsgCorePub)


@pytest.mark.nats
def test_commandsubscriber_is_coresub(messenger):
    sub = get_commandsubscriber('test_no_js.factory')
    assert isinstance(sub, MsgCommandSubscriber)
    assert isinstance(sub, MsgCoreSub)


# ---------------------------------------------------------------------------
# Core pub/sub integration
# ---------------------------------------------------------------------------

@pytest.mark.nats
async def test_core_pub_sub_async_callback(messenger, unique_subject):
    """MsgCorePub -> MsgCoreSub with an async callback."""
    subject = f'test_no_js.{unique_subject}'
    received: list[tuple[dict, dict]] = []
    event = asyncio.Event()

    async def on_message(data: dict, meta: dict) -> None:
        received.append((data, meta))
        event.set()

    pub = get_corepublisher(subject)
    sub = get_coresubscriber(subject)
    async with pub, sub:
        await sub.subscribe(on_message)
        await asyncio.sleep(0.05)
        await pub.publish(data={'value': 42})
        await asyncio.wait_for(event.wait(), timeout=3)

    assert len(received) == 1
    data, meta = received[0]
    assert data['value'] == 42
    assert 'id' in meta
    assert 'ts' in meta


@pytest.mark.nats
async def test_core_pub_sub_sync_callback(messenger, unique_subject):
    """MsgCorePub -> MsgCoreSub with a synchronous callback."""
    subject = f'test_no_js.{unique_subject}'
    received: list[dict] = []
    event = asyncio.Event()

    def on_message(data: dict, meta: dict) -> None:
        received.append(data)
        event.set()

    pub = get_corepublisher(subject)
    sub = get_coresubscriber(subject)
    async with pub, sub:
        await sub.subscribe(on_message)
        await asyncio.sleep(0.05)
        await pub.publish(data={'hello': 'world'})
        await asyncio.wait_for(event.wait(), timeout=3)

    assert len(received) == 1
    assert received[0]['hello'] == 'world'


@pytest.mark.nats
async def test_core_pub_sub_multiple_messages(messenger, unique_subject):
    """Multiple messages all delivered and preserve publish order."""
    subject = f'test_no_js.{unique_subject}'
    n = 5
    received: list[dict] = []
    done = asyncio.Event()

    async def on_message(data: dict, meta: dict) -> None:
        received.append(data)
        if len(received) >= n:
            done.set()

    pub = get_corepublisher(subject)
    sub = get_coresubscriber(subject)
    async with pub, sub:
        await sub.subscribe(on_message)
        await asyncio.sleep(0.05)
        for i in range(n):
            await pub.publish(data={'i': i})
        await asyncio.wait_for(done.wait(), timeout=5)

    assert [d['i'] for d in received] == list(range(n))


@pytest.mark.nats
async def test_core_sub_close_unsubscribes(messenger, unique_subject):
    """After MsgCoreSub.close(), further publishes are not delivered."""
    subject = f'test_no_js.{unique_subject}'
    received: list[dict] = []

    async def on_message(data: dict, meta: dict) -> None:
        received.append(data)

    pub = get_corepublisher(subject)
    sub = get_coresubscriber(subject)
    async with pub:
        async with sub:
            await sub.subscribe(on_message)
            await asyncio.sleep(0.05)
            await pub.publish(data={'seq': 1})
            await asyncio.sleep(0.1)
        await pub.publish(data={'seq': 2})
        await asyncio.sleep(0.2)

    assert len(received) == 1
    assert received[0]['seq'] == 1


# ---------------------------------------------------------------------------
# Core reader (async iterator / read_next)
# ---------------------------------------------------------------------------

@pytest.mark.nats
async def test_core_reader_async_iterator(messenger, unique_subject):
    """MsgCoreReader yields (data, meta) tuples via async iteration."""
    subject = f'test_no_js.{unique_subject}'
    n = 3
    received: list[dict] = []
    done = asyncio.Event()

    pub = get_corepublisher(subject)
    reader = get_corereader(subject)
    async with pub, reader:
        async def _consume():
            async for data, meta in reader:
                received.append(data)
                if len(received) >= n:
                    done.set()
                    break

        task = asyncio.ensure_future(_consume())
        await asyncio.sleep(0.05)
        for i in range(n):
            await pub.publish(data={'n': i})
        await asyncio.wait_for(done.wait(), timeout=5)
        task.cancel()

    assert [d['n'] for d in received] == list(range(n))


@pytest.mark.nats
async def test_core_reader_read_next(messenger, unique_subject):
    """MsgCoreReader.read_next() returns the next (data, meta) pair."""
    subject = f'test_no_js.{unique_subject}'

    pub = get_corepublisher(subject)
    reader = get_corereader(subject)
    async with pub, reader:
        await asyncio.sleep(0.05)
        await pub.publish(data={'key': 'value'})
        data, meta = await asyncio.wait_for(reader.read_next(), timeout=3)

    assert data['key'] == 'value'
    assert 'id' in meta


# ---------------------------------------------------------------------------
# Command pub/sub
# ---------------------------------------------------------------------------

@pytest.mark.nats
async def test_command_pub_sub_async_callback(messenger, unique_subject):
    """MsgCommandPublisher.command() -> MsgCommandSubscriber async callback."""
    subject = f'test_no_js.{unique_subject}'
    commands: list[tuple[str, dict, dict]] = []
    event = asyncio.Event()

    async def on_command(command: str, params: dict, meta: dict) -> None:
        commands.append((command, params, meta))
        event.set()

    pub = get_commandpublisher(subject)
    sub = get_commandsubscriber(subject)
    async with pub, sub:
        await sub.subscribe(on_command)
        await asyncio.sleep(0.05)
        await pub.command('say', text='hello world', priority=1)
        await asyncio.wait_for(event.wait(), timeout=3)

    assert len(commands) == 1
    cmd, params, meta = commands[0]
    assert cmd == 'say'
    assert params['text'] == 'hello world'
    assert params['priority'] == 1
    assert 'id' in meta


@pytest.mark.nats
async def test_command_pub_sub_sync_callback(messenger, unique_subject):
    """MsgCommandPublisher.command() -> MsgCommandSubscriber sync callback."""
    subject = f'test_no_js.{unique_subject}'
    commands: list[tuple[str, dict]] = []
    event = asyncio.Event()

    def on_command(command: str, params: dict, meta: dict) -> None:
        commands.append((command, params))
        event.set()

    pub = get_commandpublisher(subject)
    sub = get_commandsubscriber(subject)
    async with pub, sub:
        await sub.subscribe(on_command)
        await asyncio.sleep(0.05)
        await pub.command('stop')
        await asyncio.wait_for(event.wait(), timeout=3)

    assert len(commands) == 1
    cmd, params = commands[0]
    assert cmd == 'stop'
    assert params == {}
