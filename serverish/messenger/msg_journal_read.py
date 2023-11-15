from __future__ import annotations

import logging

from serverish.messenger import Messenger
from serverish.messenger.msg_reader import MsgReader
from serverish.messenger.msg_journal_pub import JournalEntry

log = logging.getLogger(__name__.rsplit('.')[-1])


class MsgJournalReader(MsgReader):
    """A class for reading journal logs published by `MsgJournalPublisher`
    """

    def __init__(self, subject, parent=None, deliver_policy='last', opt_start_time=None, consumer_cfg=None,
                 stop_when_done: bool = True,
                 **kwargs) -> None:
        super().__init__(subject, parent, deliver_policy, opt_start_time, consumer_cfg, **kwargs)

    def __aiter__(self):
        return super().__aiter__()

    async def __anext__(self) -> (JournalEntry, dict):
        data, meta = await super().__anext__()
        assert meta['message_type'].startswith('journal')

        entry = JournalEntry.from_dict(
            {k:v for k,v in data.items() if k != 'op'}
        )
        return entry, meta


def get_journalreader(subject: str,
                      deliver_policy='last',
                      **kwargs) -> 'MsgJournalReader':
    """Returns a journal reader for a given subject

    Args:
        subject: subject to read from
        deliver_policy: deliver policy,

    Returns:
        MsgJournalReader: a journal reader for the given subject
    """

    return Messenger.get_journalreader(subject=subject,
                                       deliver_policy=deliver_policy,
                                       **kwargs)
