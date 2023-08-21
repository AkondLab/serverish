from __future__ import annotations

from asyncio import Event

import nats.errors
import param
from nats.aio.subscription import Subscription
from nats.js.api import DeliverPolicy, ConsumerConfig

from serverish.messenger import Messenger
from serverish.messenger.messenger import MsgDriver, log


class MsgReader(MsgDriver):
    queue: str = param.String(default=None, allow_None=True, doc="Queue name, if None, subject is used")
    durable_name: str = param.String(default=None, allow_None=True, doc="Durable name, if None, queue is used")
    deliver_policy: str = param.ObjectSelector(default='all',
                                               objects=['all', 'last', 'new',
                                                        'by_start_sequence',
                                                        'by_start_time',
                                                        'last_per_subject'
                                                        ],
                                               doc="Delivery policy, for underlying JetStream subscription")
    opt_start_time = param.Date(default=None, allow_None=True,
                                                          doc="Start time, for underlying JetStream subscription")
    opt_start_seq: int | None = param.Integer(default=None, allow_None=True,
                                              doc="Start sequence, for underlying JetStream subscription")

    def __init__(self, **kwargs) -> None:
        self.subscription: Subscription | None = None
        self._stop: Event = Event()
        super().__init__(**kwargs)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.subscription is None:
            await self.open()

        while True:
            if self._stop.is_set():
                await self.close()
                raise StopAsyncIteration
            try:
                bmsg = await self.subscription.next_msg()
                break
            except nats.errors.TimeoutError:
                pass  # keep iterating  # TODO: Consider transformation for push based when stream gets empty
            except Exception as e:
                log.error(f"During interation on {self}, subscription.next_msg raised {e}")
                await self.close()
                raise e

        msg = self.messenger.decode(bmsg.data)
        self.messenger.log_msg_trace(msg, f"SUB iteration from {self.subject}")
        data, meta = self.messenger.split_msg(msg)
        return data, meta

    async def open(self) -> None:
        if self.subscription is not None:
            raise RuntimeError("Subscription already open, do not reuse MsgSubscription instances")

        js = self.connection.js

        # Convert the delivery policy from a string to the appropriate DeliverPolicy enum
        deliver_policy_enum = DeliverPolicy(self.deliver_policy)

        # from_time handling:
        if self.opt_start_time is None:
            opt_start_time = None
        else:
            opt_start_time = self.opt_start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        # Create the consumer configuration
        consumer_conf = ConsumerConfig(
            deliver_policy=deliver_policy_enum,
            opt_start_time=opt_start_time,
            opt_start_seq=self.opt_start_seq,
            durable_name=self.durable_name,
        )

        # Return a new Subscription
        self.subscription = await js.subscribe(self.subject, queue=self.queue, config=consumer_conf)

    async def close(self) -> None:
        if self.subscription is not None:
            await self.subscription.unsubscribe()
            self.subscription = None
        else:
            raise RuntimeError("Subscription already closed")

    async def drain(self, timeout: float = 0.0) -> None:
        """Drains the subscription, returns when no more messages are available

        Should not be called from within an iteration (use stop instead)

        Args:
            timeout (float): timeout in seconds

        """
        if self.subscription is None:
            raise RuntimeError("Subscription not open")

        await self.subscription.drain()
        self.stop()

    def stop(self) -> None:
        """Stops the subscription, returns immediately

        """
        if self.subscription is None:
            raise RuntimeError("Subscription not open")

        self._stop.set()


async def get_reader(subject, queue=None, durable_name=None, **kwargs) -> MsgReader:
    """Returns a subscription for a given subject, manages single subscription

    Args:
        subject (str): subject to subscribe to
        queue (str): queue name, if None, subject is used
        durable_name (str): durable name, if None, queue is used
        kwargs: additional arguments to pass to the connection

    Returns:
        Subscriber: a reader for the given subject

    Usage:
        async for msg in get_reader("subject"):
            print(msg)

    """
    return Messenger.get_reader(subject, queue=queue, durable_name=durable_name, **kwargs)
