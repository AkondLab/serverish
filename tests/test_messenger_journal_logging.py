import logging
import asyncio
import datetime
import logging

import pytest

from serverish.messenger import Messenger, get_publisher, get_journalreader, get_callbacksubscriber, \
    NatsJournalLoggingHandler, JournalEntry
from tests.test_connection import ci
from tests.test_nats import is_nats_running, ensure_stram_for_tests


# Test logging to NATS journal
@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_journal_logging():
    subject = 'test.messenger.test_messenger_journal_logging'
    async with Messenger().context(host='localhost', port=4222) as mess:
        await mess.purge(subject)
        logger = logging.getLogger('test_messenger_journal_logging')
        logger.setLevel(logging.DEBUG)
        handler = NatsJournalLoggingHandler(subject)
        logger.addHandler(handler)
        logger.info('test_messenger_journal_logging')
        await asyncio.sleep(0.1)
        # reading
        async with get_journalreader(subject) as reader:
            async for entry, meta in reader:
                print(entry, meta)
                assert isinstance(entry, JournalEntry)
                assert entry.message == 'test_messenger_journal_logging'
                assert entry.level == 20  # == INFO
                break


