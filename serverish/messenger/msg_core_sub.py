from __future__ import annotations

import asyncio
import logging
from typing import Callable

from nats.aio.msg import Msg
from nats.aio.subscription import Subscription

from serverish.messenger.messenger import MsgDriver

log = logging.getLogger(__name__.rsplit('.')[-1])


class MsgCoreSub(MsgDriver):
    """A class for subscribing to a subject using core NATS (no JetStream)

    This class wraps ``nc.subscribe`` with full serverish envelope handling
    (``meta``/``data`` split, validation, sender tagging).  It is the pub/sub
    analogue of :class:`MsgRpcResponder` for fire-and-forget subjects that are
    not backed by a JetStream stream.

    Usage::

        async def on_message(data: dict, meta: dict) -> None:
            print(data, meta)

        async with Messenger().context(host, port):
            sub = get_coresubscriber("svc.command.site.tts.say")
            await sub.open()
            await sub.subscribe(on_message)
            # … keep running …
            await sub.close()
    """

    def __init__(self, **kwargs) -> None:
        self.subscription: Subscription | None = None
        self._reconnect_cb = None
        self._subscribed_callback = None
        super().__init__(**kwargs)

    async def subscribe(
        self,
        callback: Callable[[dict, dict], None] | Callable[[dict, dict], asyncio.Future],
    ) -> None:
        """Subscribe to subject with a callback invoked for each message

        Args:
            callback: Called with ``(data: dict, meta: dict)`` for every message.
                May be a plain function or a coroutine function.
                Return value is ignored.
        """
        self._subscribed_callback = callback
        await self._do_subscribe(callback)

        # Register a reconnect callback only once so that the subscription is
        # automatically restored after a NATS reconnect.
        if self._reconnect_cb is None:
            async def _reconnect_cb() -> None:
                if self._subscribed_callback is not None:
                    log.info(f'Re-subscribing to {self.subject} after NATS reconnect')
                    await self._do_subscribe(self._subscribed_callback)

            self._reconnect_cb = _reconnect_cb
            self.connection.add_reconnect_cb(self._reconnect_cb)

    async def _do_subscribe(
        self,
        callback: Callable[[dict, dict], None] | Callable[[dict, dict], asyncio.Future],
    ) -> None:
        """Internal: create/re-create the NATS subscription."""
        nc = self.connection.nc

        if asyncio.iscoroutinefunction(callback):
            acb = callback
            scb = None
        else:
            scb = callback
            acb = None

        async def _cb(nats_msg: Msg) -> None:
            data, meta = self.messenger.unpack_nats_msg(nats_msg)
            try:
                if scb is not None:
                    scb(data, meta)
                else:
                    await acb(data, meta)
            except Exception as e:
                log.exception(
                    f'Error in callback {callback} for message {meta} {str(data)[:20]}: {e}'
                )

        self.subscription = await nc.subscribe(self.subject, cb=_cb)

    async def close(self) -> None:
        if self._reconnect_cb is not None:
            self.connection.remove_reconnect_cb(self._reconnect_cb)
            self._reconnect_cb = None
        self._subscribed_callback = None
        if self.subscription is not None:
            await self.subscription.unsubscribe()
            self.subscription = None
        return await super().close()


def get_coresubscriber(subject: str) -> 'MsgCoreSub':
    """Returns a core NATS subscriber for a given subject (no JetStream)

    Use this for fire-and-forget subjects that are not backed by a JetStream stream.

    Args:
        subject (str): subject to subscribe to

    Returns:
        MsgCoreSub: a core-NATS subscriber for the given subject

    Usage::

        async def on_message(data: dict, meta: dict) -> None:
            print(data, meta)

        async with Messenger().context(host, port):
            sub = get_coresubscriber("svc.command.site.tts.say")
            await sub.open()
            await sub.subscribe(on_message)
            # … keep running …
            await sub.close()
    """
    from serverish.messenger import Messenger
    return Messenger.get_coresubscriber(subject=subject)
