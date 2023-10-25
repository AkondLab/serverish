"""Tests for the journaling features of the messenger module, provided by the
`serverish.messenger.msg_journal_pub` and `serverish.messenger.msg_journal_read` modules.
"""
import asyncio

import aiodns
import pytest

from serverish.base import MessengerRequestNoResponders, MessengerRequestJetStreamSubject
from serverish.connection import Connection
from tests.test_connection import ci
from tests.test_nats import is_nats_running



@pytest.mark.asyncio  # This tells pytest this test is async
# @pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
# @pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_dns():
    resolver = aiodns.DNSResolver()
    ip = await Connection._check_host(resolver, 'google.com')
    assert ip is None
