import pytest
import socket


from serverish.connection import Connection
from serverish.connection_nats import ConnectionNATS


def is_nats_running(host='localhost', port=4222):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((host, port))
        s.shutdown(socket.SHUT_RDWR)
        return True
    except ConnectionRefusedError:
        return False
    finally:
        s.close()


@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_nats():
    c = ConnectionNATS(host='localhost', port=4222)
    async with c:
        codes = await c.diagnose(no_deduce=True)
        for s in codes.values():
            assert s == 'ok'

