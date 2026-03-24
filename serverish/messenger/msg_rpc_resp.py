from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Callable

from nats.aio.msg import Msg
from nats.aio.subscription import Subscription

from serverish.messenger import Messenger
from serverish.messenger.messenger import MsgDriver

log = logging.getLogger(__name__.rsplit('.')[-1])

@dataclass
class Rpc:
    nats_msg: Msg
    answered: bool = False
    data: dict | None = None
    meta: dict | None = None
    resp_data: dict | None = None
    resp_meta: dict | None = None

    def set_response(self, data: dict | None = None, meta: dict | None = None):
        """Sets response data and metadata

        This is the easiest method to return a response to a RPC request.
        The response data set here will be sent after callback function finishes."""
        self.resp_data = data
        self.resp_meta = meta

    async def response_now(self, data: dict | None = None, meta: dict | None = None):
        """Sets response data and metadata and sends the response immediately

        This method is useful if you want to send the response before callback function finishes.
        """
        assert self.answered is False, "Response already sent"
        self.set_response(data, meta)
        await self.send_response()

    async def send_response(self):
        self.answered = True
        messenger = Messenger() # this is singleton anyway
        msg = messenger.create_msg(data=self.resp_data, meta=self.resp_meta)
        bmsg = messenger.encode(msg)
        await self.nats_msg.respond(bmsg)


class MsgRpcResponder(MsgDriver):
    """A class for subscribing to a Messenger subject, waiting for requests and responding to them

    This class registers callback function to process messages sent by `MsgRpcRequester`.

    The responder automatically resubscribes after NATS reconnection to maintain reliability
    for server components.

    Usage:
        def callback(rpc: Rpc):
            c = rpc.data['a'] + rpc.data['b']
            rpc.set_response(data={'c': c})

        responder = MsgRpcResponder(subject='subject')
        await responder.open()
        try:
            await responder.register_function(callback)
            # ... wait for incoming messages
        finally:
            await responder.close()
    """

    def __init__(self, **kwargs) -> None:
        self.subscription: Subscription | None = None
        self._callback: Callable | None = None  # Store callback for resubscription
        self._reconnect_count: int = 0
        super().__init__(**kwargs)

    async def open(self) -> None:
        """Open the responder and register for reconnection callbacks"""
        self.connection.add_reconnect_cb(self.on_nats_reconnect)
        await super().open()

    async def on_nats_reconnect(self) -> None:
        """Handle NATS reconnection by resubscribing"""
        log.info(f"NATS reconnected, resubscribing RPC responder for {self.subject}")
        if self._callback is not None:
            await self._resubscribe()

    async def _resubscribe(self) -> None:
        """Internal method to resubscribe after connection recovery"""
        self._reconnect_count += 1
        # Clean up old subscription if exists
        if self.subscription is not None:
            try:
                await self.subscription.unsubscribe()
            except Exception as e:
                log.debug(f"Error unsubscribing during resubscribe: {e}")
            self.subscription = None

        # Re-register with the stored callback
        if self._callback is not None:
            try:
                await self._register_function_internal(self._callback)
                log.info(f"Successfully resubscribed RPC responder for {self.subject}")
            except Exception as e:
                log.error(f"Failed to resubscribe RPC responder for {self.subject}: {e}")

    async def register_function(self, callback: Callable[[Rpc], None] | Callable[[Rpc], asyncio.Future]):
        """Sets a callback function for each message

        Args:
            callback: a callback function to be called on each message. May be asynchronous.
                callback is called with one argument - Rpc object which encapsulates the incoming message
                (`data` and `meta` properties)  and allows to set or send response.
                Callback function should return None.
        """
        # Store callback for potential resubscription after reconnection
        self._callback = callback
        await self._register_function_internal(callback)

    async def _register_function_internal(self, callback: Callable[[Rpc], None] | Callable[[Rpc], asyncio.Future]):
        """Internal method to register the callback with NATS subscription"""
        from nats.aio.client import Client as NATS

        nats: NATS = self.connection.nc
        if asyncio.iscoroutinefunction(callback):
            acb = callback
            scb = None
        else:
            scb = callback
            acb = None

        async def _cb(nats_msg:Msg):

            data, meta = self.messenger.unpack_nats_msg(nats_msg)
            rpc = Rpc(nats_msg=nats_msg, data=data, meta=meta)
            ret = None
            try:
                if scb is not None:
                    ret = scb(rpc)
                else:
                    ret = await acb(rpc)
            except Exception as e:
                log.error(f'Exception occurred in {callback}')
                log.exception(f'Error in callback {callback} for message {meta}{str(data):20}: {e}')
                if not rpc.answered:
                    rpc.set_response(data=None, meta={'status': 'error', 'error': f'{e}'})
            if ret is not None:
                raise ValueError(f"Callback {callback} returned {ret}, expected None, "
                                 f"use rpc.set_response() to set return value")
            if not rpc.answered:
                await rpc.send_response()


        self.subscription = await nats.subscribe(self.subject, queue=self.subject, cb=_cb)

    async def close(self) -> None:
        """Close the responder and unregister from reconnection callbacks"""
        self.connection.remove_reconnect_cb(self.on_nats_reconnect)
        if self.subscription is not None:
            await self.subscription.unsubscribe()
        self._callback = None  # Clear callback reference
        return await super().close()

    @property
    def health_status(self) -> dict:
        """Returns current health status of the RPC responder for monitoring

        Note: RPC responder uses core NATS push subscription which is more
        susceptible to slow consumer issues than JetStream pull consumers.
        Monitor pending_messages and connection_slow_consumers for diagnostics.
        """
        # Get pending stats from subscription if available
        pending_messages = 0
        pending_bytes = 0
        if self.subscription is not None:
            try:
                pending_messages = self.subscription.pending_msgs
                pending_bytes = self.subscription.pending_bytes
            except (AttributeError, Exception):
                pass

        # Get slow consumer count from connection
        connection_slow_consumers = 0
        try:
            connection_slow_consumers = self.connection._slow_consumer_count
        except (AttributeError, Exception):
            pass

        return {
            'is_open': self.is_open,
            'subject': self.subject,
            'has_subscription': self.subscription is not None,
            'reconnect_count': self._reconnect_count,
            'pending_messages': pending_messages,
            'pending_bytes': pending_bytes,
            'connection_slow_consumers': connection_slow_consumers,
        }


def get_rpcresponder(subject: str) -> 'MsgRpcResponder':
    """Returns a callback-based subscriber RPC responder

    Args:
        subject (str): subject to read from

    Returns:
        MsgRpcResponder: a single-value reader for the given subject


    Usage:
        def callback(rpc: Rpc):
            c = rpc.data['a'] + rpc.data['b']
            rpc.set_response(data={'c': c})

        responder = MsgRpcResponder(subject='subject')
        await responder.open()
        try:
            await responder.register_function(callback)
            # ... wait for incoming messages
        finally:
            await responder.close()
    """
    return Messenger.get_rpcresponder(subject=subject)
