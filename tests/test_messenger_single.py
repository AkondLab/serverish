import logging
import asyncio
import datetime
import logging

import pytest

from serverish.messenger import Messenger, single_publish, single_read
from tests.test_connection import ci
from tests.test_nats import is_nats_running, ensure_stram_for_tests


@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_pub_single():
    async with Messenger().context(host='localhost', port=4222) as messenger:
        messenger.purge('test.messenger.test_messenger_pub_single')
        await single_publish('test.messenger.test_messenger_pub_single', data={'msg': 'test_messenger_pub_single'})


@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_pub_then_read_single():
    async with Messenger().context(host='localhost', port=4222) as messenger:
        messenger.purge('test.messenger.test_messenger_pub_then_read_single')
        data_pub = {'msg': 'test_messenger_pub_single'}
        await single_publish('test.messenger.test_messenger_pub_then_read_single', data=data_pub)
        data_read, meta_read = await single_read('test.messenger.test_messenger_pub_then_read_single')
        assert data_read == data_pub
