from __future__ import annotations

import logging
from typing import Callable
import asyncio
from asyncio import Event, CancelledError

import param

from serverish.base import Task, create_task
from serverish.messenger import Messenger
from serverish.messenger.msg_reader import MsgReader

log = logging.getLogger(__name__.rsplit('.')[-1])


class MsgCallbackSubscriber(MsgReader):
    """A class for subscribing to a Messenger subject and calling a callback function on each message

    This class works like `MsgReader`, but allows to specify a callback function for each message instead of iterating
    """
    callback = param.Callable(default=None, doc="Callback function to call on each message")
    task = param.ClassSelector(default=None, class_=Task, doc="Task for reading messages")
    _stop_event = param.ClassSelector(default=Event(), class_=Event, doc="Event to stop reading messages")

    async def open(self) -> None:
        return await super().open()

    async def close(self) -> None:
        await self.stop()
        if self.task is not None:
            self.task.cancel()
        return await super().close()

    async def stop(self) -> None:
        """Stops reading messages"""
        self._stop_event.set()

    async def subscribe(self, callback: Callable[[dict, dict], bool] | Callable[[dict, dict], asyncio.Future]) -> None:
        """Sets a callback function for each message

        Args:
            callback: a callback function to call on each message, may be asynchronous
            callback is called with two arguments: message dict and metadata dict,
            and should return True to continue reading messages, False to stop
        """
        self.callback = callback
        if asyncio.iscoroutinefunction(callback):
            self.task = await create_task(self._task_body(acb = callback), f'NATSASUB.{self.subject}')
        else:
            self.task = await create_task(self._task_body(scb= callback), f'NATSSSUB.{self.subject}')
        return self.task

    async def _task_body(self,
                         scb: Callable[[dict, dict], bool] | None = None,
                         acb: Callable[[dict, dict], asyncio.Future] | None = None
                         ) -> None:

        assert scb is not None or acb is not None
        assert not (scb is not None and acb is not None)
        cb = scb or acb
        cont = True
        log.debug(f"Entering sync interation{self}")
        async for data, meta in self:
            try:
                if scb is not None:
                    log.debug(f"Calling sync callback {cb} for message {meta}{str(data):20}")
                    cont = scb(data, meta)
                else:
                    log.debug(f"Calling async callback {cb} for message {meta}{str(data):20}")
                    cont = await acb(data, meta)
            except CancelledError:
                log.debug(f'Cancelled {self}')
                break
            except Exception as e:
                log.exception(f'Error in callback {cb} for message {meta}{str(data):20}: {e}')
            if not cont or self._stop_event.is_set() :
                break
        log.debug(f"Exiting sync interation{self}")


def get_callbacksubscriber(subject: str,
                                 deliver_policy='last',
                                 **kwargs) -> 'MsgCallbackSubscriber':
    """Returns a callback-based subscriber for a given subject

    Args:
        subject (str): subject to read from
        deliver_policy: deliver policy, in this context 'last' is most useful
        kwargs: additional arguments to pass to the consumer config

    Returns:
        MsgCallbackSubscriber: a single-value reader for the given subject

    """
    return Messenger.get_callbacksubscriber(subject=subject,
                                            deliver_policy=deliver_policy,
                                            **kwargs)
