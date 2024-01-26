import logging
import os
import pytest
import socket

from serverish.connection import ConnectionNATS


def find_nats_host(port = 4222):
    candidates = [
        'nats',
        'nats.local',
        'localhost',
        '127.0.0.1',
    ]
    for host in candidates:
        if is_nats_here(host, port):
            return host
        return None


async def is_nats_here(host, port):
    c = ConnectionNATS(host=host, port=port)
    try:
        await c.connect()
        ret = c.nc.is_connected
        await c.disconnect()
        return ret
    except Exception:
        return False


@pytest.fixture(scope="session")
def nats_host():
    return find_nats_host()

@pytest.fixture(scope="session")
def nats_port():
        return 4222  # NATS port (the same on CI and local environment)
