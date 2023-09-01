from __future__ import annotations

import logging
from typing import Callable
import asyncio
from asyncio import Event, CancelledError

import param
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription

from serverish.base import Task, create_task
from serverish.messenger import Messenger
from serverish.messenger.messenger import MsgDriver
from serverish.messenger.msg_callback_sub import MsgCallbackSubscriber
from serverish.messenger.msg_reader import MsgReader

log = logging.getLogger(__name__.rsplit('.')[-1])


class MsgRpcResponder(MsgDriver):
    """A class for subscribing to a Messenger subject, waiting for requests and responding to them

    """

    def __init__(self, **kwargs) -> None:
        self.subscription: Subscription | None = None
        super().__init__(**kwargs)

    async def register_function(self, callback: Callable[[dict, dict], (dict, dict)] | Callable[[dict, dict], asyncio.Future]) -> Task:
        """Sets a callback function for each message

        Args:
            callback: a callback function to call on each message, may be asynchronous
            callback is called with two arguments: message dict and metadata dict,
            and should return True to continue reading messages, False to stop
        """
        from nats.aio.client import Client as NATS

        nats: NATS = self.connection.nc
        if asyncio.iscoroutinefunction(callback):
            acb = callback
            scb = None
        else:
            scb = callback
            acb = None

        async def _cb(nats_msg:Msg):

            bmsg = nats_msg.data
            msg = self.messenger.decode(bmsg)
            log.debug(f"Received Request message {msg}")
            data, meta = self.messenger.split_msg(msg)
            try:
                if scb is not None:
                    rdata, rmeta = scb(data, meta)
                else:
                    rdata, rmeta = await acb(data, meta)
            except Exception as e:
                log.exception(f'Error in callback {callback} for message {meta}{data:20}: {e}')
                rdata = {}
                rmeta = {'status': 'error', 'error': f'{e}'}
            msg = self.messenger.create_msg(data=rdata, meta=rmeta)
            bmsg = self.messenger.encode(msg)
            await nats_msg.respond(bmsg)


        self.subscription = await nats.subscribe(self.subject, queue=self.subject, cb=_cb)

    async def close(self) -> None:
        if self.subscription is not None:
            await self.subscription.unsubscribe()
        return await super().close()


def get_rpcresponder(subject: str) -> 'MsgRpcResponder':
    """Returns a callback-based subscriber RPC responder

    Args:
        subject (str): subject to read from

    Returns:
        MsgRpcResponder: a single-value reader for the given subject


    Usage:
        r = async get_rpcresponder("subject"):
        r.open()
        r register_function(callback)
        #...
        r.close()
    """
    return Messenger.get_rpcresponder(subject=subject)
