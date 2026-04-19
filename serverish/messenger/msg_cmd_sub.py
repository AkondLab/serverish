from __future__ import annotations

import asyncio
import logging
from typing import Callable

from serverish.messenger.msg_core_sub import MsgCoreSub

log = logging.getLogger(__name__.rsplit('.')[-1])


class MsgCommandSubscriber(MsgCoreSub):
    """A subscriber for fire-and-forget commands over core NATS (no JetStream)

    Wraps :class:`MsgCoreSub` with a convenience :meth:`subscribe` override that
    unpacks the command envelope and calls the callback with
    ``(command: str, params: dict, meta: dict)``.

    The expected data envelope (as produced by :class:`MsgCommandPublisher`) is::

        {"command": "<name>", "params": {<keyword arguments>}}

    Usage::

        async def on_command(command: str, params: dict, meta: dict) -> None:
            if command == "say":
                print(params["text"])

        async with Messenger().context(host, port):
            sub = get_commandsubscriber("svc.command.site.tts")
            await sub.open()
            await sub.subscribe(on_command)
            # … keep running …
            await sub.close()
    """

    async def subscribe(
        self,
        callback: Callable[[str, dict, dict], None] | Callable[[str, dict, dict], asyncio.Future],
    ) -> None:
        """Subscribe with a command callback

        Args:
            callback: Called with ``(command: str, params: dict, meta: dict)``
                for every received message.  ``command`` is the value of
                ``data['command']``, ``params`` is the value of
                ``data['params']`` (defaults to ``{}`` if absent), and ``meta``
                is the standard serverish metadata dict.  May be a plain
                function or a coroutine function.  Return value is ignored.
        """
        if asyncio.iscoroutinefunction(callback):
            async def _wrapper(data: dict, meta: dict) -> None:
                command = data.get('command', '')
                params = data.get('params', {})
                try:
                    await callback(command, params, meta)
                except Exception as e:
                    log.exception(
                        f'Error in command callback {callback} '
                        f'for command {command!r} meta={meta}: {e}'
                    )
        else:
            async def _wrapper(data: dict, meta: dict) -> None:
                command = data.get('command', '')
                params = data.get('params', {})
                try:
                    callback(command, params, meta)
                except Exception as e:
                    log.exception(
                        f'Error in command callback {callback} '
                        f'for command {command!r} meta={meta}: {e}'
                    )

        await super().subscribe(_wrapper)


def get_commandsubscriber(subject: str) -> 'MsgCommandSubscriber':
    """Returns a command subscriber for a given subject (core NATS, no JetStream)

    Args:
        subject (str): subject to subscribe to

    Returns:
        MsgCommandSubscriber: a command subscriber for the given subject

    Usage::

        async def on_command(command: str, params: dict, meta: dict) -> None:
            if command == "say":
                print(params["text"])

        async with Messenger().context(host, port):
            sub = get_commandsubscriber("svc.command.site.tts")
            await sub.open()
            await sub.subscribe(on_command)
            # … keep running …
            await sub.close()
    """
    from serverish.messenger import Messenger
    return Messenger.get_commandsubscriber(subject=subject)
