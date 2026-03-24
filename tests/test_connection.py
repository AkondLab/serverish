import os

import pytest
import socket

from serverish.connection import Connection
from serverish.base.status import StatusEnum


def _can_ping(host: str, port: int) -> bool:
    """Check if we can actually reach a host — stricter than DNS-only check."""
    try:
        with socket.create_connection((host, port), timeout=3):
            return True
    except OSError:
        return False


@pytest.mark.skipif(
    not _can_ping('google.com', 80) or bool(os.getenv('CI')),
    reason='requires unrestricted internet (ping to google.com:80)',
)
@pytest.mark.timeout(15)
async def test_connection_diagnostics_all_positive():
    c = Connection('google.com', 80)
    codes = await c.diagnose(no_deduce=True)
    for c, s in codes.items():
        assert s == StatusEnum.ok


@pytest.mark.skipif(
    not _can_ping('1.1.1.1', 80) or bool(os.getenv('CI')),
    reason='requires unrestricted internet (ping to 1.1.1.1:80)',
)
@pytest.mark.timeout(15)
async def test_connection_diagnostics_all_positive_ip():
    c = Connection('1.1.1.1', 80)
    codes = await c.diagnose(no_deduce=True)
    for c, s in codes.items():
        if c == 'dns':
            assert s == 'na'
        else:
            assert s == 'ok'
