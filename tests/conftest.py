"""Test fixtures for serverish test suite.

Provides NATS server management (testcontainers or local fallback),
session-scoped Messenger lifecycle, test isolation via unique subjects,
and a NatsDisruptor for failure injection tests.
"""
from __future__ import annotations

import asyncio
import logging
import socket
import time
import uuid

import pytest
import pytest_asyncio
from testcontainers.nats import NatsContainer

from serverish.messenger import Messenger

logger = logging.getLogger(__name__)


def _is_nats_available(host: str, port: int) -> bool:
    """Check if a NATS server is reachable at the given host and port."""
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except (ConnectionRefusedError, OSError):
        return False


@pytest.fixture(scope='session')
def nats_server():
    """Provide a NATS server, preferring a local one, falling back to testcontainers.

    Yields a dict with keys: host, port, container (None if using local server).
    """
    for host in ('localhost', '127.0.0.1'):
        if _is_nats_available(host, 4222):
            logger.info('Using existing NATS server at %s:4222', host)
            yield {'host': host, 'port': 4222, 'container': None}
            return

    logger.info('No local NATS server found, starting testcontainer')
    container = NatsContainer(image='nats:latest', jetstream=True)
    try:
        container.start()
        tc_host, tc_port = container.nats_host_and_port()
        logger.info('Started NATS testcontainer at %s:%d', tc_host, tc_port)
        yield {'host': tc_host, 'port': tc_port, 'container': container}
    finally:
        container.stop()


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def messenger(nats_server):
    """Provide a session-scoped Messenger connected to the NATS server.

    When using a testcontainer (fresh NATS), creates the 'test' stream with
    'test.>' subject wildcard to match the test subject naming convention.
    """
    m = Messenger()
    await m.open(host=nats_server['host'], port=nats_server['port'])
    # Ensure the 'test.>' stream exists — needed for purge, find_stream, etc.
    # On a long-lived local NATS this stream likely already exists;
    # on a fresh testcontainer it must be created.
    js = m.connection.js
    try:
        await js.find_stream_name_by_subject('test.probe')
    except Exception:
        from nats.js.api import StreamConfig
        await js.add_stream(StreamConfig(
            name='test',
            subjects=['test.>'],
            storage='memory',
            max_msgs=10000,
        ))
        logger.info("Created 'test' stream with 'test.>' subject wildcard")
        # Refresh connection status so is_open reflects the new stream
        await m.connection.update_statuses()
    yield m
    await m.close()


@pytest_asyncio.fixture(autouse=True, scope='module', loop_scope='session')
async def reset_messenger_state(messenger):
    """Reset Messenger driver children between test modules for isolation."""
    yield
    for child in list(messenger.children_by_name.values()):
        try:
            await child.close()
        except Exception as e:
            logger.warning('Error closing child driver during reset: %s', e)
    messenger.children_by_name.clear()
    messenger.children_names.clear()
    # Refresh connection statuses — closing children can leave JetStream
    # status checks stale, causing is_open to return False.
    if messenger.conn is not None:
        await messenger.connection.update_statuses()


@pytest.fixture
def unique_subject(request):
    """Generate a unique NATS subject for test isolation."""
    test_name = request.node.name
    uid = uuid.uuid4().hex[:8]
    return f'test.{test_name}.{uid}'


class NatsDisruptor:
    """Controls a dedicated NATS container for failure injection tests.

    Provides pause, unpause, and restart operations on the container
    to simulate network disruptions and server failures.
    """

    def __init__(self, container: NatsContainer) -> None:
        self._container = container
        self.host = container.get_container_host_ip()
        self.port = int(container.get_exposed_port(4222))

    def pause(self) -> None:
        """Pause the NATS container, simulating a network partition."""
        self._container.get_wrapped_container().pause()

    def unpause(self) -> None:
        """Unpause the NATS container, restoring connectivity."""
        self._container.get_wrapped_container().unpause()

    def restart(self) -> None:
        """Restart the NATS container, simulating a server crash and recovery."""
        self._container.get_wrapped_container().restart()


@pytest.fixture
def nats_disruptor():
    """Provide a NatsDisruptor with its own dedicated NATS container."""
    container = NatsContainer(image='nats:latest', jetstream=True)
    try:
        container.start()
        yield NatsDisruptor(container)
    finally:
        container.stop()


@pytest_asyncio.fixture(loop_scope='session')
async def resilience_messenger(nats_disruptor, nats_server):
    """Connect Messenger singleton to a disruptor container for resilience testing.

    Closes the current Messenger connection, reopens it against the disruptor
    container with a short ping_interval (2s) so that the nats-py client can
    detect frozen connections quickly during pause/unpause tests.
    Creates the 'test.>' stream on the fresh container and yields the Messenger
    instance.  Teardown closes the disruptor connection and reopens the Messenger
    against the original session-scoped NATS server.
    """
    from serverish.connection.connection_jets import ConnectionJetStream
    from serverish.base import create_task

    m = Messenger()
    await m.close()

    # Build connection manually so we can pass ping_interval to nats-py
    conn = ConnectionJetStream(nats_disruptor.host, nats_disruptor.port)
    await conn.update_statuses()
    m.conn = conn
    opener = await create_task(
        conn.connect(ping_interval=2, max_outstanding_pings=2),
        f'Resilience NATS connection {nats_disruptor.host}:{nats_disruptor.port}',
    )
    await opener.wait_for(timeout=15)

    js = m.connection.js
    from nats.js.api import StreamConfig
    await js.add_stream(StreamConfig(
        name='test',
        subjects=['test.>'],
        storage='memory',
        max_msgs=10000,
    ))
    yield m
    await m.close()

    # Reopen the Messenger against the original session-scoped NATS server
    # so that subsequent tests using the `messenger` fixture work correctly.
    await m.open(host=nats_server['host'], port=nats_server['port'])
    # Wait for JetStream status to settle — the initial status check may
    # time out if the connection is not fully ready yet.
    for _ in range(10):
        await m.connection.update_statuses()
        if m.is_open:
            break
        await asyncio.sleep(0.5)


async def wait_for_healthy(driver, timeout: float = 15.0, check_interval: float = 0.3) -> dict:
    """Poll driver.health_status until it indicates recovery.

    Returns the final health_status dict on success.
    Raises TimeoutError if driver does not recover within timeout.
    """
    start = time.monotonic()
    last_status = {}
    while time.monotonic() - start < timeout:
        last_status = driver.health_status
        if last_status.get('is_open') and not last_status.get('last_error'):
            return last_status
        await asyncio.sleep(check_interval)
    raise TimeoutError(
        f'Driver did not recover within {timeout}s. Last status: {last_status}'
    )
