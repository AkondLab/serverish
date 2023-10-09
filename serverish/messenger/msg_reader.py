from __future__ import annotations

import logging
import asyncio
from asyncio import Event
from datetime import datetime

import nats.errors
import param
from nats.js import JetStreamContext
from nats.js.api import DeliverPolicy, ConsumerConfig

from serverish.base import wait_for_psce
from serverish.base.exceptions import MessengerReaderStopped
from serverish.base.fifoset import FifoSet
from serverish.messenger import Messenger
from serverish.messenger.messenger import MsgDriver

log = logging.getLogger(__name__.rsplit('.')[-1])


class MsgReader(MsgDriver):
    """A class for reading data from a Messenger subject

    Use this class if you want to read data from a messenger subject.
    Check for specialist readers for common use cases.
    """

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
        self._stop: Event = Event()
        self._push: Event = Event()
        self._msg_processed: Event = Event()
        self.id_cache = FifoSet(128)
        super().__init__(subject=subject, parent=parent,
                         deliver_policy=deliver_policy, opt_start_time=opt_start_time, consumer_cfg=consumer_cfg,
                         **kwargs)
        log.debug(f"Created {self}")

    def __aiter__(self):
        log.debug(f"Entering iteration {self}")
        return self

    async def __anext__(self):
        try:
            data, meta = await self.read_next()
            return data, meta
        except MessengerReaderStopped:
            raise StopAsyncIteration


    async def read_next(self) -> tuple[dict, dict]:
        if not self.is_open:
            await self.open()

        bmsg = None
        pushed = None

        # First - pull subscription
        if not self._push.is_set():
            while True:
                try:
                    log.debug(f"Pulling from {self}")
                    bmsg = (await self.pull_subscription.fetch(batch=1, timeout=0.1))[0]
                    await bmsg.ack()
                    msg = self.messenger.decode(bmsg.data)
                    self.messenger.log_msg_trace(msg, f"SUB PULL iteration from {self.subject}")
                    data, meta = self.messenger.split_msg(msg)
                    pushed = False
                except nats.errors.TimeoutError:
                    if self.push_subscription is None:
                        await self._transform_to_push_sub()
                        # we are creating push subscription but still pulling messages if possible to do not miss any message
                        continue
                    else:
                        # push is ready and pull timeouts, we can switch to push
                        self._push.set()
                break

        # Second - push subscription
        if bmsg is None and self._push.is_set():
            log.debug(f"Waiting for pushed to {self}")
            while True:
                if self._stop.is_set():
                    await self.close()
                    raise MessengerReaderStopped
                try:
                    bmsg = await self.push_subscription.next_msg()
                    await bmsg.ack()
                    msg = self.messenger.decode(bmsg.data)
                    self.messenger.log_msg_trace(msg, f"SUB PUSH iteration from {self.subject}")
                    data, meta = self.messenger.split_msg(msg)
                    if meta['id'] in self.id_cache:
                        log.debug(f"Skipping duplicated message from {self.subject}")
                        bmsg = msg = data = meta = None
                        continue
                    break
                except nats.errors.TimeoutError:
                    pass  # keep iterating
                except Exception as e:
                    log.error(f"During interation on {self}, push subscription.next_msg raised {e}")
                    await self.close()
                    raise e

        if bmsg is None:
            log.debug(f"No message to return, closing {self}")
            await self.close()
            raise MessengerReaderStopped
        try:
            self.id_cache.add(meta['id'])
        except Exception as e:
            log.error(f"Error adding id of msg: data={data} meta={meta} to id_cache: {e}")
        log.debug(f"Returning message {self}")
        self._msg_processed.set()
        return data, meta

    async def open(self) -> None:
        if self.pull_subscription is not None:
            raise RuntimeError("Reader already open, do not reuse MsgReader instances")

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
        await super().open()


    async def _transform_to_push_sub(self):
        """After fetching existing messages, switch to push based consumer"""
        log.debug(f"Attempt to transform {self} to push subscription")
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
        log.debug(f"{self}:  push subscription set up")
        # self._push.set()

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
                # TODO: Check if it is proper format for NATS!
                cfg['opt_start_time'] = self.opt_start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        # Create the pull consumer configuration
        consumer_conf = ConsumerConfig(**cfg)
        return consumer_conf

    async def close(self) -> None:
        await super().close()
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

    async def wait_for_empty(self, timeout: float | None = None) -> None:
        """Waits for the subscription to be empty, returns when no more messages are currently available

        Should not be called from within an iteration

        Args:
            timeout (float): timeout in seconds

        """
        now = datetime.now()
        if not self.is_open:
            raise RuntimeError("Subscription not open")

        # wait for pull subscription to finish ot stop
        tw = asyncio.create_task(self._push.wait())
        ts = asyncio.create_task(self._stop.wait())
        await asyncio.wait([tw, ts],
                           timeout=timeout,
                           return_when=asyncio.FIRST_COMPLETED
                           )
        tw.cancel()
        ts.cancel()
        if self._stop.is_set():
            return
        elif self._push.is_set():
            self._msg_processed.clear()
            while self.push_subscription.pending_msgs > 0:
                to = timeout - (datetime.now() - now).total_seconds() if timeout is not None else None
                await wait_for_psce(self._msg_processed.wait(), timeout=to)
                self._msg_processed.clear()



    async def drain(self, timeout: float | None = None) -> None:
        """Drains then CLOSES the subscription, returns when no more messages are available

        Should not be called from within an iteration (use stop instead)

        Args:
            timeout (float): timeout in seconds

        """
        if not self.is_open:
            raise RuntimeError("Subscription not open")

        # wait for pull subscription to finish ot stop
        tw = asyncio.create_task(self._push.wait())
        ts = asyncio.create_task(self._stop.wait())
        await asyncio.wait([tw, ts],
                           timeout=timeout,
                           return_when=asyncio.FIRST_COMPLETED
                           )
        tw.cancel()
        ts.cancel()
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


def get_reader(subject: str,
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
