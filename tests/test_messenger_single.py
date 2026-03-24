import logging

import pytest

from serverish.messenger import Messenger, single_publish, single_read


@pytest.mark.nats
async def test_messenger_pub_single(messenger, unique_subject):
    await messenger.purge(unique_subject)
    await single_publish(unique_subject, data={'msg': 'test_messenger_pub_single'})


@pytest.mark.nats
async def test_messenger_pub_then_read_single(messenger, unique_subject):
    await messenger.purge(unique_subject)
    data_pub = {'msg': 'test_messenger_pub_single'}
    await single_publish(unique_subject, data=data_pub)
    data_read, meta_read = await single_read(unique_subject)
    assert data_read == data_pub
