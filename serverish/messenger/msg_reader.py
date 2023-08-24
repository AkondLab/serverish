from __future__ import annotations

import logging
import asyncio
from asyncio import Event

import nats.errors
import param
from nats.aio.subscription import Subscription
from nats.js import JetStreamContext
from nats.js.api import DeliverPolicy, ConsumerConfig

from serverish.messenger import Messenger
from serverish.messenger.messenger import MsgDriver

log = logging.getLogger(__name__.rsplit('.')[-1])


class MsgReader(MsgDriver):
    deliver_policy: str = param.ObjectSelector(default='all',
                                               objects=['all', 'last', 'new',
                                                        'by_start_sequence',
                                                        'by_start_time',
                                                        'last_per_subject'
                                                        ],
                                               doc="Delivery policy, for underlying JetStream subscription")
    opt_start_time = param.Date(default=None, allow_None=True,
                                                          doc="Start time, for underlying JetStream subscription")
    consumer_cfg = param.Dict(default={}, allow_None=False,
                                 doc="Additional JetStream consumer configuration")

    def __init__(self, subject, parent = None,
                 deliver_policy = 'all',
                 opt_start_time = None,
                 consumer_cfg=None,
                 **kwargs) -> None:
        if parent is None:
            parent = Messenger()
        if consumer_cfg is None:
            consumer_cfg = {}
        self.pull_subscription: JetStreamContext.PullSubscription | None = None
        self.push_subscription: JetStreamContext.PushSubscription | None = None
        self.is_open: bool = False
        self._stop: Event = Event()
        self._push: Event = Event()
        super().__init__(subject=subject, parent=parent,
                         deliver_policy=deliver_policy, opt_start_time=opt_start_time, consumer_cfg=consumer_cfg,
                         **kwargs)
        log.debug(f"Created {self}")

    def __aiter__(self):
        log.debug(f"Entering iteration {self}")
        return self

    async def __anext__(self):
        if not self.is_open:
            await self.open()

        bmsg = None
        pushed = None

        # First - pull subscription
        if not self._push.is_set():
            try:
                log.debug(f"Pulling from {self}")
                bmsg = (await self.pull_subscription.fetch(batch=1, timeout=0.1))[0]
                await bmsg.ack()
                pushed = False
            except nats.errors.TimeoutError:
                await self._transform_pull_to_push()

        # Second - push subscription
        if bmsg is None and self._push.is_set():
            log.debug(f"Waiting for pushed to {self}")
            while True:
                if self._stop.is_set():
                    await self.close()
                    raise StopAsyncIteration
                try:
                    bmsg = await self.push_subscription.next_msg()
                    break
                except nats.errors.TimeoutError:
                    pass  # keep iterating
                except Exception as e:
                    log.error(f"During interation on {self}, subscription.next_msg raised {e}")
                    await self.close()
                    raise e

        if bmsg is None:
            log.debug(f"No message to return, closing {self}")
            await self.close()
            raise StopAsyncIteration
        msg = self.messenger.decode(bmsg.data)
        self.messenger.log_msg_trace(msg, f"SUB iteration from {self.subject}")
        data, meta = self.messenger.split_msg(msg)
        log.debug(f"Returning message {self}")
        return data, meta

    async def open(self) -> None:
        if self.pull_subscription is not None:
            raise RuntimeError("Subscription already open, do not reuse MsgSubscription instances")

        log.debug(f"Opening {self}")
        js = self.connection.js

        consumer_conf = await self._create_consumer_cfg()

        # check weather to create pull consumer first
        # (i.e. existing messages are in our interest, durable name is obligatory)
        if consumer_conf.deliver_policy != DeliverPolicy.NEW:     #and consumer_conf.durable_name is not None:
            log.debug(f"Creating pull subscription for {self}")
            consumer_conf.durable_name = self.name if consumer_conf.durable_name is None else consumer_conf.durable_name
            self.pull_subscription = await js.pull_subscribe(self.subject,
                                                             durable=consumer_conf.durable_name,
                                                             config=consumer_conf)
        else:
            log.debug(f"Creating push subscription for {self}")
            self.push_subscription = await js.subscribe(self.subject,
                                                        config=consumer_conf)
            self._push.set()
        self.is_open = True


    async def _transform_pull_to_push(self):
        """After fetching existing messages, switch to push based consumer"""
        log.debug(f"Attempt to transform {self} from pull to push")
        pull = self.pull_subscription
        if pull is None: # no transition needed or not possible
            log.debug(f"No pull - no transform {self}")
            return

        consumer_conf = await self._create_consumer_cfg()
        consumer_conf.deliver_policy = DeliverPolicy.NEW
        js = self.connection.js

        push: JetStreamContext.PushSubscription = await js.subscribe(
            self.subject, config=consumer_conf
        )
        self.push_subscription = push
        await self._close_pull_subscription()
        log.debug(f"Transformed {self} from pull to push")
        self._push.set()

    async def _create_consumer_cfg(self):
        cfg = self.consumer_cfg.copy()
        # Convert the delivery policy from a string to the appropriate DeliverPolicy enum
        if self.deliver_policy is not None:
            cfg['deliver_policy'] = DeliverPolicy(self.deliver_policy)
        # from_time handling:
        if self.opt_start_time is not None:
            if isinstance(self.opt_start_time, str):
                cfg['opt_start_time'] = self.opt_start_time
            else:
                cfg['opt_start_time'] = self.opt_start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        # Create the pull consumer configuration
        consumer_conf = ConsumerConfig(**cfg)
        return consumer_conf

    async def close(self) -> None:
        self.is_open = False
        await self._close_pull_subscription()
        await self._close_push_subscription()

    async def _close_pull_subscription(self) -> None:
        if self.pull_subscription is not None:
            ci = await self.pull_subscription.consumer_info()
            await self.pull_subscription.unsubscribe()
            await self.connection.js.delete_consumer(stream=ci.stream_name, consumer=ci.name)
            self.pull_subscription = None

    async def _close_push_subscription(self) -> None:
        if self.push_subscription is not None:
            await self.push_subscription.unsubscribe()
            self.push_subscription = None

    async def drain(self, timeout: float | None = None) -> None:
        """Drains the subscription, returns when no more messages are available

        Should not be called from within an iteration (use stop instead)

        Args:
            timeout (float): timeout in seconds

        """
        if not self.is_open:
            raise RuntimeError("Subscription not open")

        if timeout is not None:
            # Calculate the remaining timeout after waiting for events
            start_time = asyncio.get_event_loop().time()

        # wait for pull subscription to finish ot stop
        await asyncio.wait([self._push.wait(), self._stop.wait()],
                           timeout=timeout,
                           return_when=asyncio.FIRST_COMPLETED
                           )
        if self._stop.is_set():
            return
        elif self._push.is_set():
            await self.push_subscription.drain()
        self.stop()

    def stop(self) -> None:
        """Stops the subscription, returns immediately

        """
        if not self.is_open:
            raise RuntimeError("Subscription not open")

        self._stop.set()


async def get_reader(subject: str,
                   deliver_policy='all',
                   opt_start_time=None,
                   **kwargs) -> 'MsgReader':
    """Returns a subscription for a given subject, manages single subscription

    Args:
        subject (str): subject to subscribe to
        kwargs: additional arguments to pass to the consumer config

    Returns:
        Subscriber: a reader for the given subject

    Usage:
        async for msg in get_reader("subject"):
            print(msg)

    """
    return Messenger.get_reader(subject=subject,
                                deliver_policy=deliver_policy,
                                opt_start_time=opt_start_time,
                                **kwargs)
