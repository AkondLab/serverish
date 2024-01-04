import nats
import pytest

from serverish.messenger import  Messenger, get_reader
from tests.test_connection import ci
from tests.test_nats import is_nats_running


@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_issue5_subject_not_in_stream():

    async with Messenger().context(host='localhost', port=4222) as mess:
        reader = get_reader("notexsisting.stream", deliver_policy="last")
        try:
            cfg = await reader.read_next()
        except nats.js.errors.NotFoundError:
            pass
        else:
            assert False, 'Shoud raise NotFoundError'
