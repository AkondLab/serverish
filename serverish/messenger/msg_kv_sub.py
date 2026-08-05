from __future__ import annotations

import asyncio
from asyncio import CancelledError, Event
from typing import Callable

import param

from serverish.base import Task, create_task
from serverish.messenger.messenger import Messenger
from serverish.messenger.msg_kv_read import MsgKvReader
from serverish.messenger.msg_kv import log


class MsgKvSubscriber(MsgKvReader):
    """A class for watching KV keys and calling a callback function on each change

    This class works like `MsgKvReader`, but allows to specify a callback function
    for each change instead of iterating.

    The callback is called with (data, meta); data is None for delete/purge markers
    (check meta['kv']['operation']). Note that current values of matching keys are
    delivered on subscribe, before live updates.

    Usage:
        def callback(data, meta):
            print(meta['kv']['key'], data)

        sub = get_kvsubscriber('bucket', key='telescope.>')
        await sub.open()
        await sub.subscribe(callback)
    """
    callback = param.Callable(default=None, doc="Callback function to call on each change")
    task = param.ClassSelector(default=None, class_=Task, doc="Task for watching changes")

    def __init__(self, **kwargs) -> None:
        self._stop_event = Event()
        super().__init__(**kwargs)

    async def close(self) -> None:
        await self.stop()
        if self.task is not None:
            self.task.cancel()
        return await super().close()

    async def stop(self) -> None:
        """Stops watching changes"""
        self._stop_event.set()

    async def subscribe(self, callback: Callable[[dict | None, dict], bool] |
                                        Callable[[dict | None, dict], asyncio.Future]) -> Task:
        """Sets a callback function for each change of watched keys

        Args:
            callback: a callback function to call on each change, may be asynchronous
            callback is called with two arguments: data dict (None for delete/purge
            markers) and metadata dict, and may return False to stop watching.
            Any other return value (including None — i.e. callbacks with no explicit
            return — and True) keeps the subscription running.
        """
        self.callback = callback
        if asyncio.iscoroutinefunction(callback):
            self.task = await create_task(self._task_body(acb=callback), f'KVASUB.{self.bucket}.{self.key}')
        else:
            self.task = await create_task(self._task_body(scb=callback), f'KVSSUB.{self.bucket}.{self.key}')
        return self.task

    async def _task_body(self,
                         scb: Callable[[dict | None, dict], bool] | None = None,
                         acb: Callable[[dict | None, dict], asyncio.Future] | None = None
                         ) -> None:

        assert scb is not None or acb is not None
        assert not (scb is not None and acb is not None)
        cb = scb or acb
        cont: object = True
        log.debug(f"Entering KV watch iteration {self}")
        async for data, meta in self:
            try:
                if scb is not None:
                    log.debug(f"Calling sync callback {cb} for KV change {meta}{str(data):20}")
                    cont = scb(data, meta)
                else:
                    log.debug(f"Calling async callback {cb} for KV change {meta}{str(data):20}")
                    cont = await acb(data, meta)
            except CancelledError:
                log.debug(f'Cancelled {self}')
                break
            except Exception as e:
                log.exception(f'Error in callback {cb} for KV change {meta}{str(data):20}: {e}')
            # Stop only on explicit False — None (the implicit "no return" value
            # of a Python function) and any truthy value keep the subscription
            # alive. Treating None as "stop" would silently kill any callback
            # that doesn't bother returning anything, which is the common case.
            if cont is False or self._stop_event.is_set():
                break
        log.debug(f"Exiting KV watch iteration {self}")


def get_kvsubscriber(bucket: str, key: str = '>', **kwargs) -> MsgKvSubscriber:
    """Returns a callback-based KV subscriber for a given bucket and key pattern

    Args:
        bucket (str): KV bucket name
        key (str): key or wildcard pattern to watch
        kwargs: additional driver arguments (e.g. include_history, ignore_deletes)

    Returns:
        MsgKvSubscriber: a callback-based KV subscriber for the given bucket
    """
    return Messenger.get_kvsubscriber(bucket, key=key, **kwargs)
