from __future__ import annotations

import logging
import asyncio
from asyncio import Event
from collections import deque
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

class _ReconnectNeededError(Exception):
    pass

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

    # behaviour config
    on_connection_close = param.ObjectSelector(default="WAIT", objects=["RAISE", "FINISH", "WAIT"],
                                               doc="On disconnection: "
                                                   "RAISE - reraise exception, "
                                                   "FINISH - silently finish iteration, "
                                                   "WAIT - wait for connection to be re-established")
    on_missed_messages = param.ObjectSelector(default="SKIP", objects=["SKIP", "REPLAY"],
                                              doc="On missed message (e.g. during broken connection): "
                                                  "SKIP - skip delayed messages"
                                                  "REPLAY - read and replay")

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
        self.last_seq: int | None = None
        self._stop: Event = Event()
        self._emptied: Event = Event()
        self._msg_processed: Event = Event()
        self._reconnect_needed: Event = Event()
        self.pull_batch = deque()
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
        finally:
            pass


    async def read_next(self) -> tuple[dict, dict]:
        if not self.is_open:
            await self.open()


        n = 0
        while len(self.pull_batch) == 0:
            if self._stop.is_set():
                await self.close()
                raise MessengerReaderStopped
            if n > 2:
                self._emptied.set()

            try:
                timeout = min(0.2 + n/5, 5)  # start fast and slow down to 5s
                batch = 1 if n > 1 else 100
                log.debug(f"Pulling {batch} in {timeout}s from {self}")
                self.pull_batch = deque(await self.pull_subscription.fetch(batch=batch, timeout=timeout))
                log.debug(f"Pulled {len(self.pull_batch)} messages from {self}")
            except nats.errors.TimeoutError:
                pass
            n += 1

        self._emptied.clear()
        bmsg = self.pull_batch.popleft()
        await bmsg.ack()
        data, meta = self.messenger.unpack_nats_msg(bmsg)
        self.messenger.log_msg_trace(data, meta, f"SUB PULL iteration from {self.subject}")
        meta['receive_mode'] = 'pull'
        logging.debug(f'Returning message {data}')

        return data, meta


    async def read_next_(self) -> tuple[dict, dict]:
        if not self.is_open:
            await self.open()


        keep_pulling = True
        transform_to_pull = False
        while keep_pulling:
            if transform_to_pull:
                try:
                    await self._transform_to_pull_sub()
                    keep_pulling = False
                    transform_to_pull = False
                except (nats.errors.ConnectionClosedError, nats.errors.TimeoutError):
                    await asyncio.sleep(1)
                    continue
            else:
                keep_pulling = False

            bmsg = None
            data, meta = {}, {}
            # First - pull subscription
            if not self._emptied.is_set():
                # self.pull_batch = deque()
                while True:
                    if self._stop.is_set():
                        await self.close()
                        raise MessengerReaderStopped
                    try:
                        if len(self.pull_batch) == 0:
                            log.debug(f"Pulling from {self}")
                            self.pull_batch = deque(await self.pull_subscription.fetch(batch=100, timeout=10.0))
                            log.debug(f"Pulled {len(self.pull_batch)} messages from {self}")
                        bmsg = self.pull_batch.popleft()
                        await bmsg.ack()
                        data, meta = self.messenger.unpack_nats_msg(bmsg)
                        self.messenger.log_msg_trace(data, meta, f"SUB PULL iteration from {self.subject}")
                        meta['receive_mode'] = 'pull'
                    except nats.errors.TimeoutError:
                        if self.push_subscription is None:
                            await self._transform_to_push_sub()
                            # we are creating push subscription but still pulling messages if possible to do not miss any message
                            continue
                        else:
                            # push is ready and pull timeouts, we can switch to push
                            self._emptied.set()
                    except nats.errors.ConnectionClosedError:
                        if self.on_connection_close == 'RAISE':
                            log.warning(f'Connection closed, raising exception on {self}')
                            raise
                        elif self.on_connection_close == 'FINISH':
                            log.warning(f'Connection closed, finishing iteration on {self}')
                            self._stop.set()
                        else:
                            assert self.on_connection_close == 'WAIT'
                            log.warning(f'Connection closed, waiting for reconnect on {self}')
                            await asyncio.sleep(1.0)
                            continue
                    break

            # Second - push subscription
            if bmsg is None and self._emptied.is_set():
                log.debug(f"Waiting for pushed to {self}")
                while True:
                    if self._stop.is_set():
                        await self.close()
                        raise MessengerReaderStopped
                    try:
                        if self._reconnect_needed.is_set():
                            self._reconnect_needed.clear()
                            raise _ReconnectNeededError
                        bmsg = await self.push_subscription.next_msg()
                        await bmsg.ack()
                        data, meta = self.messenger.unpack_nats_msg(bmsg)
                        self.messenger.log_msg_trace(data, meta, f"SUB PUSH iteration from {self.subject}")
                        meta['receive_mode'] = 'push'
                        if f'{meta["id"]}{meta["ts"]}' in self.id_cache:
                            log.debug(f"Skipping duplicated message from {self.subject}")
                            bmsg = data = meta = None
                            continue
                        break
                    except nats.errors.TimeoutError:
                        pass  # keep iterating
                    except (nats.errors.ConnectionClosedError, _ReconnectNeededError):
                        if self.on_connection_close == 'RAISE':
                            log.warning(f'Connection closed, raising exception on {self}')
                            raise
                        elif self.on_connection_close == 'FINISH':
                            log.warning(f'Connection closed, finishing iteration on {self}')
                            self._stop.set()
                        else:
                            assert self.on_connection_close == 'WAIT'
                            log.warning(f'Connection closed, waiting for reconnect on {self}')
                            await asyncio.sleep(1.0)
                            transform_to_pull = True
                            keep_pulling = True
                            break

                    except Exception as e:
                        log.error(f"During interation on {self}, push subscription.next_msg raised {e}")
                        await self.close()
                        raise e

        if bmsg is None:
            log.debug(f"No message to return, closing {self}")
            await self.close()
            raise MessengerReaderStopped
        cachedid = f'{meta["id"]}{meta["ts"]}'
        try:
            self.id_cache.add(cachedid)
        except Exception as e:
            log.error(f"Error adding id: {cachedid} of msg: data={data} meta={meta} to id_cache: {e}")
        log.debug(f"Returning message {self}")
        self.last_seq = meta['nats']['seq']
        self._msg_processed.set()
        return data, meta

    async def open(self) -> None:
        if self.pull_subscription is not None:
            raise RuntimeError("Reader already open, do not reuse MsgReader instances")

        log.debug(f"Opening {self}")
        js = self.connection.js

        self.connection.add_reconnect_cb(self.on_nats_reconnect)


        consumer_conf = await self._create_consumer_cfg()

        log.debug(f"Creating pull subscription for {self}")
        self.pull_subscription = await self._create_pull_subscribtion(consumer_conf)

        # if consumer_conf.deliver_policy != DeliverPolicy.NEW:
        #     log.debug(f"Creating pull subscription for {self}")
        #     self.pull_subscription = await self._create_pull_subscribtion(consumer_conf)
        # else:
        #     log.debug(f"Creating push subscription for {self}")
        #     self.push_subscription = await js.subscribe(self.subject,
        #                                                 config=consumer_conf)

        # self._emptied.set()
        await super().open()

    async def _create_pull_subscribtion(self, consumer_conf: ConsumerConfig):
        # Durable consumer is probably not needed (at least problematic)
        # consumer_conf.durable_name = self.name if consumer_conf.durable_name is None else consumer_conf.durable_name
        return await self.connection.js.pull_subscribe(self.subject,
                                                       durable=consumer_conf.durable_name,
                                                       config=consumer_conf)

    async def _transform_to_pull_sub(self):
        log.info(f"Attempt to transform {self.subject} MeassageReader mode to PULL (from seq: {self.last_seq})")
        try:
            await self.push_subscription.unsubscribe()
        except (nats.errors.ConnectionClosedError, AttributeError):  # no connection, no reason to break
            pass
        self.push_subscription = None
        self._emptied.clear()
        consumer_conf = await self._create_consumer_cfg()
        # set policy for 'new messages' for push subscription
        if self.last_seq is not None:
            consumer_conf.deliver_policy = DeliverPolicy.BY_START_SEQUENCE
            consumer_conf.opt_start_time = None
            consumer_conf.opt_start_seq = self.last_seq + 1
        else:
            consumer_conf.deliver_policy = DeliverPolicy.NEW
            consumer_conf.opt_start_time = None
            consumer_conf.opt_start_seq = None
        self.pull_subscription = await self._create_pull_subscribtion(consumer_conf=consumer_conf)
        log.info(f"{self}:  PULL subscription set up "
                 f"(policy={consumer_conf.deliver_policy}, seq={consumer_conf.opt_start_seq})")



    async def _transform_to_push_sub(self):
        """After fetching existing messages, switch to push based consumer"""
        log.info(f"Attempt to transform {self.subject} MeassageReader mode to PUSH for future messages")
        pull = self.pull_subscription
        if pull is None: # no transition needed or not possible
            log.debug(f"No pull - no transform {self}")
            return

        consumer_conf = await self._create_consumer_cfg()
        # set policy for 'new messages' for push subscription
        consumer_conf.deliver_policy = DeliverPolicy.NEW
        consumer_conf.opt_start_time = None
        consumer_conf.opt_start_seq = None
        js = self.connection.js

        push: JetStreamContext.PushSubscription = await js.subscribe(
            self.subject, config=consumer_conf
        )
        self.push_subscription = push
        log.info(f"{self}:  PUSH subscription set up")
        # self._push.set()

    async def _create_consumer_cfg(self) -> ConsumerConfig:
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
        Messenger().connection.remove_reconnect_cb(self.on_nats_reconnect)
        await super().close()
        await self._close_pull_subscription()
        await self._close_push_subscription()

    async def _close_pull_subscription(self) -> None:
        if self.pull_subscription is not None:
            try:
                ci = await self.pull_subscription.consumer_info()
                await self.pull_subscription.unsubscribe()
                await self.connection.js.delete_consumer(stream=ci.stream_name, consumer=ci.name)
            except nats.js.errors.NotFoundError:
                pass  # no consumer is ok
            except Exception as e:
                log.warning(f'Exception while closing PULL subscription: {e}')
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
        tw = asyncio.create_task(self._emptied.wait())
        ts = asyncio.create_task(self._stop.wait())
        await asyncio.wait([tw, ts],
                           timeout=timeout,
                           return_when=asyncio.FIRST_COMPLETED
                           )
        tw.cancel()
        ts.cancel()
        if self._stop.is_set():
            return
        elif self._emptied.is_set():
            self._msg_processed.clear()
            while self.push_subscription is not None and self.push_subscription.pending_msgs > 0:
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
        tw = asyncio.create_task(self._emptied.wait())
        ts = asyncio.create_task(self._stop.wait())
        await asyncio.wait([tw, ts],
                           timeout=timeout,
                           return_when=asyncio.FIRST_COMPLETED
                           )
        tw.cancel()
        ts.cancel()
        if self._stop.is_set():
            return
        elif self._emptied.is_set():
            await self.push_subscription.drain()
        self.stop()

    def stop(self) -> None:
        """Stops the subscription, returns immediately

        """
        if not self.is_open:
            raise RuntimeError("Subscription not open")

        self._stop.set()

    def reconnect(self) -> None:
        self._reconnect_needed.set()

    async def on_nats_reconnect(self) -> None:
        self.reconnect()

    def is_pull(self):
        return not self._emptied.is_set()

    def __str__(self):
        return f"[{'PULL' if self.is_pull() else 'PUSH'}]{super().__str__()}"



def get_reader(subject: str,
                   deliver_policy='all',
                   opt_start_time=None,
                   **kwargs) -> 'MsgReader':
    """Returns a subscription for a given subject, manages single subscription

    Args:
        subject (str): subject to subscribe to
        deliver_policy (str): deliver policy, one of 'all', 'last', 'new', 'by_start_time', will be passed to consumer config
        opt_start_time (datetime): start time for 'by_start_time' deliver policy, will be passed to consumer config
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
