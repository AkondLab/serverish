"""
Based on `rich.progress`, in many cases just plugging this class in place of `rich.progress.Progress` works.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Sequence

import param

from serverish.base import dt_ensure_array, dt_utcnow_array
from serverish.base.idmanger import gen_uid
from serverish.messenger import Messenger
from serverish.messenger.messenger import MsgDriver
from serverish.messenger.msg_publisher import MsgPublisher


@dataclass
class JournalEntry:
    conversation_id: str
    level: int
    message: str
    timestamp: list[int]
    explanation: str = field(default=None)
    icon: str = field(default=None)
    actions: list[str] = field(default_factory=list)
    driver: MsgDriver = field(default=None)

    def to_dict(self):
        d = {
            'conversation_id': self.conversation_id,
            'level': self.level,
            'message': self.message,
            'timestamp': self.timestamp
        }
        if self.explanation:
            d['explanation'] = self.explanation
        if self.icon:
            d['icon'] = self.icon
        if self.actions:
            d['actions'] = self.actions
        return d

    @classmethod
    def from_dict(cls, data):
        return cls(**data)


class MsgJournalPublisher(MsgPublisher):
    """A class for publishing journal messages and updates to a subject

    Use this class for text based, log-like messages to be displayed for a user.
    Interface mimics `logging` module, but note, that methods are async.
    Note, that raise_on_publish_error is set to False by default, which differs from the default in MsgPublisher.
    """
    raise_on_publish_error = param.Boolean(default=False)  # override default
    conversations = param.Dict(default={}, doc="Conversations being tracked")

    _nameToLevel = None

    @classmethod
    def checkLevel(cls, level: int | str):
        """Copied from logging module"""
        if isinstance(level, int):
            rv = level
        elif str(level) == level:
            if cls._nameToLevel is None:
                try:
                    cls._nameToLevel = logging.getLevelNamesMapping()
                except AttributeError:  # Python 3.11 introduced getLevelNamesMapping
                    cls._nameToLevel = logging._nameToLevel
                if 'NOTICE' not in cls._nameToLevel:
                    cls._nameToLevel['NOTICE'] = 25
            if level not in cls._nameToLevel:
                raise ValueError("Unknown level: %r" % level)
            rv = cls._nameToLevel[level]
        else:
            raise TypeError("Level not an integer or a valid string: %r"
                            % (level,))
        return rv

    async def log(self,
                  level: int | str,
                  message: str,
                  *args,
                  explanation: str | None = None,
                  icon: str | None = None,
                  actions: list[str] | None = None,
                  timestamp: datetime | Sequence | None = None,
                  meta: dict | None = None
                  ) -> JournalEntry:
        """Log a message to the journal.

        Args:
            level (int): Message level (as in logging module)
            message (str): Message text
            args: arguments to be applied to message using `str.format`
            explanation (str, optional): Detailed explanation. Defaults to None.
            icon (str, optional): Icon to display. Defaults to None.
            actions (list[str], optional): List of actions to display. Defaults to None.
            timestamp (datetime | Sequence | None, optional): Timestamp of the message. Defaults to None.
            meta (dict, optional): Additional metadata to publish. Defaults to None.
        """
        if timestamp is None:
            timestamp = dt_utcnow_array()
        entry = JournalEntry(
            driver=self,
            conversation_id=gen_uid('journal'),
            level=self.checkLevel(level),
            message=message.format(*args),
            timestamp=dt_ensure_array(timestamp),
            explanation=explanation,
            icon=icon,
            actions=actions
        )
        return await self.publish_journal_operation(entry, op='publish', meta=meta)

    async def debug(self, message: str, *args, **kwargs) -> JournalEntry:
        """Log a message with level DEBUG on the journal."""
        return await self.log(logging.DEBUG, message, *args, **kwargs)

    async def info(self, message: str, *args, **kwargs) -> JournalEntry:
        """Log a message with level INFO on the journal."""
        return await self.log(logging.INFO, message, *args, **kwargs)

    async def notice(self, message: str, *args, **kwargs) -> JournalEntry:
        """Log a message with level NOTICE on the journal."""
        return await self.log('NOTICE', message, *args, **kwargs)

    async def warning(self, message: str, *args, **kwargs) -> JournalEntry:
        """Log a message with level WARNING on the journal."""
        return await self.log(logging.WARNING, message, *args, **kwargs)

    async def error(self, message: str, *args, **kwargs) -> JournalEntry:
        """Log a message with level ERROR on the journal."""
        return await self.log(logging.ERROR, message, *args, **kwargs)

    async def critical(self, message: str, *args, **kwargs) -> JournalEntry:
        """Log a message with level CRITICAL on the journal."""
        return await self.log(logging.CRITICAL, message, *args, **kwargs)

    async def publish_journal_operation(self, entry: JournalEntry, op: str, meta=None) -> JournalEntry:
        """Publish a journal operation

        Args:
            entry (JournalEntry): Journal entry to publish
            op (str): Operation to perform on the journal entry. One of 'publish', 'update', 'close'
            meta (dict, optional): Additional metadata to publish. Defaults to None.
        """
        if meta is None:
            meta = {}
        meta.update({'message_type': f'journal.{op}'})
        dentry = entry.to_dict()
        dentry['op'] = op
        await self.publish(dentry, meta = meta)
        return entry




def get_journalpublisher(subject) -> MsgJournalPublisher:
    """Returns a publisher for a given subject

    Args:
        subject (str): subject to publish to

    Returns:
        MsgJournalPublisher: a publisher for the given subject

    """
    return Messenger.get_journalpublisher(subject)
