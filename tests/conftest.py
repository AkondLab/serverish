"""Test fixtures for serverish test suite.

Provides NATS server management (testcontainers or local fallback),
session-scoped Messenger lifecycle, test isolation via unique subjects,
and a NatsDisruptor for failure injection tests.
"""
from __future__ import annotations

import logging
import socket
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
    container = NatsContainer(image='nats:latest')
    container.with_command('-js')
    try:
        container.start()
        host, port = container.get_exposed_port(4222), 4222
        # testcontainers maps to a random host port
        tc_host = container.get_container_host_ip()
        tc_port = int(container.get_exposed_port(4222))
        logger.info('Started NATS testcontainer at %s:%d', tc_host, tc_port)
        yield {'host': tc_host, 'port': tc_port, 'container': container}
    finally:
        container.stop()


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def messenger(nats_server):
    """Provide a session-scoped Messenger connected to the NATS server."""
    m = Messenger()
    await m.open(host=nats_server['host'], port=nats_server['port'])
    yield m
    await m.close()


@pytest_asyncio.fixture(autouse=True, scope='module', loop_scope='module')
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
    container = NatsContainer(image='nats:latest')
    container.with_command('-js')
    try:
        container.start()
        yield NatsDisruptor(container)
    finally:
        container.stop()
