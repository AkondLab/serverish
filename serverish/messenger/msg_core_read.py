from __future__ import annotations

import asyncio
import logging

import param
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription

from serverish.base.exceptions import MessengerReaderStopped, MessengerNotConnected
from serverish.connection.nats_errors import nats_errors_translated
from serverish.messenger.messenger import MsgDriver, subject_family

log = logging.getLogger(__name__.rsplit('.')[-1])


class MsgCoreReader(MsgDriver):
    """A class for reading data from a subject using core NATS (no JetStream)

    Use this class if you want to iterate over messages published to a subject
    over core NATS (fire-and-forget, no stream backing required).  It is the
    core-NATS counterpart of :class:`MsgReader` and supports the same async
    iteration protocol.

    Call :meth:`open` to start receiving, then iterate with ``async for``::

        async with Messenger().context(host, port):
            reader = get_corereader("svc.event.sensor.reading")
            async with reader:
                async for data, meta in reader:
                    print(data, meta)

    Or use :meth:`read_next` directly::

        await reader.open()
        try:
            while True:
                data, meta = await reader.read_next()
                process(data, meta)
        except MessengerReaderStopped:
            pass
        finally:
            await reader.close()
    """

    family = param.Boolean(default=False,
                           doc="Read the subject family: the subject and all subjects extending it with "
                               "further tokens")

    def __init__(self, **kwargs) -> None:
        self._queue: asyncio.Queue | None = None
        self._stop: asyncio.Event = asyncio.Event()
        self.subscriptions: list[Subscription] = []
        self._reconnect_cb = None
        super().__init__(**kwargs)
        if self.family:
            subject_family(self.subject)  # validates: fails early for a subject which cannot have a family

    @property
    def subscription(self) -> Subscription | None:
        """The underlying subscription (the first one, a subject family uses two)"""
        return self.subscriptions[0] if self.subscriptions else None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def open(self) -> None:
        """Start receiving messages from the subject.

        Creates an internal asyncio queue, subscribes to the subject via
        ``nc.subscribe``, and registers an automatic reconnect callback.
        """
        self._queue = asyncio.Queue()
        self._stop.clear()
        await self._do_subscribe()

        if self._reconnect_cb is None:
            async def _reconnect_cb() -> None:
                log.info(f'Re-subscribing {self} after NATS reconnect')
                await self._do_subscribe()

            self._reconnect_cb = _reconnect_cb
            self.connection.add_reconnect_cb(self._reconnect_cb)

        await super().open()

    async def close(self) -> None:
        """Stop receiving messages and clean up the NATS subscription."""
        self._stop.set()
        if self._reconnect_cb is not None:
            self.connection.remove_reconnect_cb(self._reconnect_cb)
            self._reconnect_cb = None
        subscriptions, self.subscriptions = self.subscriptions, []
        with nats_errors_translated(f"Unsubscribing from '{self.subject}' failed"):
            for subscription in subscriptions:
                await subscription.unsubscribe()
        return await super().close()

    # ------------------------------------------------------------------
    # Internal subscription helper
    # ------------------------------------------------------------------

    async def _do_subscribe(self) -> None:
        """(Re-)create the raw NATS subscription, routing messages into the queue."""
        nc = self.connection.nc
        if nc is None:
            raise MessengerNotConnected(f"Cannot subscribe to '{self.subject}', NATS not connected")

        async def _cb(nats_msg: Msg) -> None:
            if self._queue is None:
                return
            try:
                data, meta = self.messenger.unpack_nats_msg(nats_msg)
            except Exception as e:
                log.exception(f'{self} error unpacking message: {e}')
                return
            await self._queue.put((data, meta))

        # a subject family takes two subscriptions feeding one queue: no single
        # pattern matches both 'a.b' and 'a.b.c'
        patterns = subject_family(self.subject) if self.family else [self.subject]
        with nats_errors_translated(f"Subscribing to '{self.subject}' failed"):
            self.subscriptions = [await nc.subscribe(pattern, cb=_cb) for pattern in patterns]

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    async def read_next(self) -> tuple[dict, dict]:
        """Return the next ``(data, meta)`` pair from the subject.

        Blocks until a message arrives or the reader is stopped.

        Returns:
            tuple[dict, dict]: data and meta from the serverish envelope.

        Raises:
            MessengerReaderStopped: when :meth:`close` has been called.
        """
        while True:
            if self._stop.is_set():
                raise MessengerReaderStopped
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                self._queue.task_done()
                return item
            except asyncio.TimeoutError:
                continue  # re-check _stop

    # ------------------------------------------------------------------
    # Async iteration protocol
    # ------------------------------------------------------------------

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return await self.read_next()
        except MessengerReaderStopped:
            raise StopAsyncIteration


def get_corereader(subject: str, family: bool = False) -> 'MsgCoreReader':
    """Returns a core NATS reader (async iterator) for a given subject (no JetStream)

    Use this for subjects that are not backed by a JetStream stream.

    Args:
        subject (str): subject to subscribe to
        family (bool): read the subject family - the subject itself and all subjects extending it
            with further tokens, but not its siblings

    Returns:
        MsgCoreReader: a core-NATS async iterator for the given subject

    Usage::

        async with Messenger().context(host, port):
            async with get_corereader("svc.event.sensor.reading") as reader:
                async for data, meta in reader:
                    print(data, meta)
    """
    from serverish.messenger import Messenger
    return Messenger.get_corereader(subject=subject, family=family)
