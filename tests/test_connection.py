import pytest
import socket

from serverish.connection import Connection
from serverish.base.status import StatusEnum


def internet_on():
    try:
        socket.create_connection(("1.1.1.1", 53))  # Cloudflare DNS, should be always accessible
        return True
    except OSError:
        pass
    return False


@pytest.mark.skipif(not internet_on(), reason="requires internet")
async def test_connection_diagnostics_all_positive():
    c = Connection('google.com', 80)
    codes = await c.diagnose(no_deduce=True)
    for c, s in codes.items():
        assert s == StatusEnum.ok


@pytest.mark.skipif(not internet_on(), reason="requires internet")
async def test_connection_diagnostics_all_positive_ip():
    c = Connection('1.1.1.1', 80)
    codes = await c.diagnose(no_deduce=True)
    for c, s in codes.items():
        if c == 'dns':
            assert s == 'na'
        else:
            assert s == 'ok'
