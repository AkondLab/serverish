import pytest
import socket


from serverish.connection import Connection


def internet_on():
    try:
        socket.create_connection(("1.1.1.1", 53))  # Cloudflare DNS, should be always accessible
        return True
    except OSError:
        pass
    return False


@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(not internet_on(), reason="requires internet")
async def test_connection_diagnostics_all_positive():
    c = Connection('google.com', 80)
    codes = await c.diagnose(no_deduce=True)
    print(codes)
