import logging
import os
import pytest
import socket


def find_nats_host(port = 4222):
    candidates = [
        'nats',
        'nats.local',
        'localhost',
        '127.0.0.1',
    ]
    for host in candidates:
        if is_port_open(host, port):
            return host
        return None

def is_port_open(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((host, port))
        return True
    except (ConnectionRefusedError, OSError):
        return False
    finally:
        s.close()


@pytest.fixture(scope="session")
def nats_host():
    return find_nats_host()

@pytest.fixture(scope="session")
def nats_port():
        return 4222  # NATS port (the same on CI and local environment)
