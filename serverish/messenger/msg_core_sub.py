from __future__ import annotations

import asyncio
import logging
from asyncio import CancelledError
from typing import Callable

from serverish.base import Task, create_task
from serverish.messenger.msg_core_read import MsgCoreReader

log = logging.getLogger(__name__.rsplit('.')[-1])


class MsgCoreSub(MsgCoreReader):
    """A class for subscribing to a subject using core NATS (no JetStream)

    This class extends :class:`MsgCoreReader` with a callback-based interface,
    analogous to :class:`MsgCallbackSubscriber` for JetStream.  It wraps
    ``nc.subscribe`` with full serverish envelope handling (``meta``/``data``
    split, validation, sender tagging) and auto-resubscribes on NATS reconnect.

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
        self._task: Task | None = None
        super().__init__(**kwargs)

    async def subscribe(
        self,
        callback: Callable[[dict, dict], None] | Callable[[dict, dict], asyncio.Future],
    ) -> None:
        """Subscribe to the subject with a callback invoked for each message

        :meth:`open` must be called before :meth:`subscribe`.

        Args:
            callback: Called with ``(data: dict, meta: dict)`` for every
                message.  May be a plain function or a coroutine function.
                Return value is ignored.
        """
        if asyncio.iscoroutinefunction(callback):
            self._task = await create_task(
                self._task_body(acb=callback), f'NATSCORESUB.{self.subject}'
            )
        else:
            self._task = await create_task(
                self._task_body(scb=callback), f'NATSCORESUB.{self.subject}'
            )
        return self._task

    async def _task_body(
        self,
        scb: Callable[[dict, dict], None] | None = None,
        acb: Callable[[dict, dict], asyncio.Future] | None = None,
    ) -> None:
        assert scb is not None or acb is not None
        cb = scb or acb
        log.debug(f'Entering core sub iteration {self}')
        async for data, meta in self:
            try:
                if scb is not None:
                    scb(data, meta)
                else:
                    await acb(data, meta)
            except CancelledError:
                log.debug(f'Cancelled {self}')
                break
            except Exception as e:
                log.exception(
                    f'Error in callback {cb} for message {meta} {str(data)[:20]}: {e}'
                )
        log.debug(f'Exiting core sub iteration {self}')

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None
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
