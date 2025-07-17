from __future__ import annotations

import logging
import asyncio
from asyncio import Event
from time import time

import nats.errors
import param
from nats.aio.subscription import Subscription
from nats.js import JetStreamContext
from nats.js.api import DeliverPolicy, ConsumerConfig

from serverish.messenger import Messenger
from serverish.messenger.messenger import MsgDriver
from serverish.messenger.msg_reader import MsgReader
from serverish.base.exceptions import MessengerReaderAlreadyOpen

log = logging.getLogger(__name__.rsplit('.')[-1])


class MsgSingleReader(MsgReader):

    def __init__(self, subject, parent = None,
                 deliver_policy = 'last',
                 **kwargs) -> None:
        super().__init__(subject=subject, parent=parent,
                         deliver_policy=deliver_policy,
                         **kwargs)

    async def read(self, wait: float | None = None) -> tuple[dict, dict]:
        """Reads a single message from the subject, call it only once.
        Automatically opens and closes the reader after reading.

        Args:
            wait (float): if it is allowed that data has not been published yet, wait for it for given time

        Returns:
            tuple: data, meta

        """
        interval = 0.1
        max_delay = 5.0
        # abolute utc timeout for waiting for data (now (utc) + wait):
        timeout = time() + wait if wait is not None else None

        while True:
            try:
                await self.open()
                break
            except MessengerReaderAlreadyOpen:
                break
            except Exception as e: # pragma: no cover
                now = time()
                if timeout is None or now > timeout:
                    log.error(f"Failed to open single reader for subject {self.subject}: {e}")
                    raise e
                log.warning(f"Failed to open single reader for subject {self.subject}: {e}, retrying in {interval} seconds")
                remaining = timeout - time() if timeout is not None else None
                await asyncio.sleep(min(interval, remaining, max_delay) if remaining is not None else min(interval, max_delay))
                interval *= 2

        try:
            data, meta = await asyncio.wait_for(self.read_next(), timeout=wait)
        finally:
            await self.close()
        return data, meta


def get_singlereader(subject: str,
                           deliver_policy='last',
                           **kwargs) -> 'MsgSingleReader':
    """Returns a single value reader for a given subject

    Args:
        subject (str): subject to read from
        deliver_policy: deliver policy, in this context 'last' is most useful
        kwargs: additional arguments to pass to the consumer config

    Returns:
        MsgSingleReader: a single-value reader for the given subject


    Usage:
        r = async get_singlereader("subject"):
            print(r.read())

    """
    return Messenger.get_singlereader(subject=subject,
                                      deliver_policy=deliver_policy,
                                      **kwargs)


async def single_read(subject: str,
                      wait: float | None = None,
                      deliver_policy='last',
                      **kwargs) -> tuple[dict, dict]:
    """Reads a single message from the subject.
    Automatically opens and closes the reader after reading.

    Args:
        subject (str): subject to read from
        wait (float): if it is allowed that data has not been published yet, wait for it for given time
        deliver_policy: deliver policy, in this context 'last' is most useful
        kwargs: additional arguments to pass to the consumer config

    Returns:
        tuple: data, meta

    Usage:
        try:
            print(await single_read("subject"))
        except MessengerReaderStopped:
            print("No data published yet")

    """
    reader = get_singlereader(subject, deliver_policy, **kwargs)
    return await reader.read(wait=wait)