"""Tests for core NATS pub/sub helpers (no JetStream).

These tests require a running NATS server on localhost:4222 and are skipped
when none is available (or when running in CI where JetStream is disabled).
"""
import asyncio

import pytest

from serverish.messenger import (
    Messenger,
    MsgCorePub,
    MsgCoreSub,
    MsgCommandPublisher,
    MsgCommandSubscriber,
    get_corepublisher,
    get_coresubscriber,
    get_commandpublisher,
    get_commandsubscriber,
)
from tests.test_connection import ci
from tests.test_nats import is_nats_running

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

_SUBJECT_PREFIX = "test_no_js.core_pubsub"


def _subject(name: str) -> str:
    return f"{_SUBJECT_PREFIX}.{name}"


# ---------------------------------------------------------------------------
# factory / constructor smoke tests (no NATS connection required)
# ---------------------------------------------------------------------------


def test_get_corepublisher_returns_instance():
    pub = get_corepublisher("svc.command.test")
    assert isinstance(pub, MsgCorePub)


def test_get_coresubscriber_returns_instance():
    sub = get_coresubscriber("svc.command.test")
    assert isinstance(sub, MsgCoreSub)


def test_get_commandpublisher_returns_instance():
    pub = get_commandpublisher("svc.command.test")
    assert isinstance(pub, MsgCommandPublisher)


def test_get_commandsubscriber_returns_instance():
    sub = get_commandsubscriber("svc.command.test")
    assert isinstance(sub, MsgCommandSubscriber)


def test_commandpublisher_is_corepub():
    pub = get_commandpublisher("svc.command.test")
    assert isinstance(pub, MsgCorePub)


def test_commandsubscriber_is_coresub():
    sub = get_commandsubscriber("svc.command.test")
    assert isinstance(sub, MsgCoreSub)


# ---------------------------------------------------------------------------
# integration tests — require a live NATS server
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="NATS not available on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_core_pub_sub_async_callback():
    """MsgCorePub → MsgCoreSub with an async callback."""
    subject = _subject("test_core_pub_sub_async_callback")
    received: list[tuple[dict, dict]] = []
    event = asyncio.Event()

    async def on_message(data: dict, meta: dict) -> None:
        received.append((data, meta))
        event.set()

    async with Messenger().context(host="localhost", port=4222):
        pub = get_corepublisher(subject)
        sub = get_coresubscriber(subject)
        async with pub, sub:
            await sub.subscribe(on_message)
            await asyncio.sleep(0.05)  # give subscription time to register
            await pub.publish(data={"value": 42})
            await asyncio.wait_for(event.wait(), timeout=3)

    assert len(received) == 1
    data, meta = received[0]
    assert data["value"] == 42
    assert "id" in meta
    assert "ts" in meta


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="NATS not available on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_core_pub_sub_sync_callback():
    """MsgCorePub → MsgCoreSub with a synchronous callback."""
    subject = _subject("test_core_pub_sub_sync_callback")
    received: list[dict] = []
    event = asyncio.Event()

    def on_message(data: dict, meta: dict) -> None:
        received.append(data)
        event.set()

    async with Messenger().context(host="localhost", port=4222):
        pub = get_corepublisher(subject)
        sub = get_coresubscriber(subject)
        async with pub, sub:
            await sub.subscribe(on_message)
            await asyncio.sleep(0.05)
            await pub.publish(data={"hello": "world"})
            await asyncio.wait_for(event.wait(), timeout=3)

    assert len(received) == 1
    assert received[0]["hello"] == "world"


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="NATS not available on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_core_pub_sub_multiple_messages():
    """Multiple messages are all delivered."""
    subject = _subject("test_core_pub_sub_multiple_messages")
    received: list[dict] = []
    done = asyncio.Event()
    N = 5

    async def on_message(data: dict, meta: dict) -> None:
        received.append(data)
        if len(received) >= N:
            done.set()

    async with Messenger().context(host="localhost", port=4222):
        pub = get_corepublisher(subject)
        sub = get_coresubscriber(subject)
        async with pub, sub:
            await sub.subscribe(on_message)
            await asyncio.sleep(0.05)
            for i in range(N):
                await pub.publish(data={"i": i})
            await asyncio.wait_for(done.wait(), timeout=5)

    assert len(received) == N
    assert [d["i"] for d in received] == list(range(N))


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="NATS not available on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_command_publisher_subscriber_async():
    """MsgCommandPublisher.command() → MsgCommandSubscriber async callback."""
    subject = _subject("test_command_publisher_subscriber_async")
    commands: list[tuple[str, dict, dict]] = []
    event = asyncio.Event()

    async def on_command(command: str, params: dict, meta: dict) -> None:
        commands.append((command, params, meta))
        event.set()

    async with Messenger().context(host="localhost", port=4222):
        pub = get_commandpublisher(subject)
        sub = get_commandsubscriber(subject)
        async with pub, sub:
            await sub.subscribe(on_command)
            await asyncio.sleep(0.05)
            await pub.command("say", text="hello world", priority=1)
            await asyncio.wait_for(event.wait(), timeout=3)

    assert len(commands) == 1
    cmd, params, meta = commands[0]
    assert cmd == "say"
    assert params["text"] == "hello world"
    assert params["priority"] == 1
    assert "id" in meta


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="NATS not available on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_command_publisher_subscriber_sync():
    """MsgCommandPublisher.command() → MsgCommandSubscriber sync callback."""
    subject = _subject("test_command_publisher_subscriber_sync")
    commands: list[tuple[str, dict]] = []
    event = asyncio.Event()

    def on_command(command: str, params: dict, meta: dict) -> None:
        commands.append((command, params))
        event.set()

    async with Messenger().context(host="localhost", port=4222):
        pub = get_commandpublisher(subject)
        sub = get_commandsubscriber(subject)
        async with pub, sub:
            await sub.subscribe(on_command)
            await asyncio.sleep(0.05)
            await pub.command("stop")
            await asyncio.wait_for(event.wait(), timeout=3)

    assert len(commands) == 1
    cmd, params = commands[0]
    assert cmd == "stop"
    assert params == {}


@pytest.mark.asyncio
@pytest.mark.skipif(ci, reason="NATS not available on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_core_sub_context_manager():
    """MsgCoreSub.close() unsubscribes cleanly (no more messages after close)."""
    subject = _subject("test_core_sub_context_manager")
    received: list[dict] = []

    async def on_message(data: dict, meta: dict) -> None:
        received.append(data)

    async with Messenger().context(host="localhost", port=4222):
        pub = get_corepublisher(subject)
        sub = get_coresubscriber(subject)
        async with pub, sub:
            await sub.subscribe(on_message)
            await asyncio.sleep(0.05)
            await pub.publish(data={"seq": 1})
            await asyncio.sleep(0.1)
        # sub is now closed — this message must NOT be received
        await pub.open()
        await pub.publish(data={"seq": 2})
        await asyncio.sleep(0.2)
        await pub.close()

    assert len(received) == 1
    assert received[0]["seq"] == 1
