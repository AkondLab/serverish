import logging
import asyncio

import pytest

from serverish.messenger import Messenger, get_publisher, get_journalreader, get_callbacksubscriber, \
    NatsJournalLoggingHandler, JournalEntry


# Test logging to NATS journal
@pytest.mark.nats
async def test_messenger_journal_logging(messenger, unique_subject):
    await messenger.purge(unique_subject)
    logger = logging.getLogger('test_messenger_journal_logging')
    logger.setLevel(logging.DEBUG)
    handler = NatsJournalLoggingHandler(unique_subject)
    logger.addHandler(handler)
    logger.info('test_messenger_journal_logging')
    await asyncio.sleep(0.1)
    # reading
    async with get_journalreader(unique_subject) as reader:
        async for entry, meta in reader:
            print(entry, meta)
            assert isinstance(entry, JournalEntry)
            assert entry.message == 'test_messenger_journal_logging'
            assert entry.level == 20  # == INFO
            break
