from __future__ import annotations

import logging
import asyncio
from asyncio import Event
from collections import deque
from datetime import datetime
import time
from dataclasses import dataclass, field
import functools

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
    error_behavior = param.ObjectSelector(default="WAIT", objects=["RAISE", "FINISH", "WAIT"],
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
        consumer_cfg_defaults = {
            'inactive_threshold': 60
        }
        if consumer_cfg is not None:
            consumer_cfg_defaults.update(consumer_cfg)
        self.batch = 100
        self.messages = deque()
        self.pull_subscription: JetStreamContext.PullSubscription | None = None
        self.push_subscription: JetStreamContext.PushSubscription | None = None
        self.last_seq: int | None = None
        self._stop: Event = Event()
        self._emptied: Event = Event()
        self._msg_processed: Event = Event()
        self._reconnect_needed: Event = Event()
        self.pull_batch = deque()
        self.id_cache = FifoSet(128)
        self._expect_beeing_open = False
        super().__init__(subject=subject, parent=parent,
                         deliver_policy=deliver_policy, opt_start_time=opt_start_time, consumer_cfg=consumer_cfg_defaults,
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
        """By default, once you enter method will not return or raise exception until have some data for you

        Only stop signal and Cancel exception finish the iteration.
        One may change this behavior by setting `on_connection_close` to 'RAISE' or 'FINISH'.
        This will cause raising exception or finishing iteration on seruios erros.
        """

        # The reader loop's state and methods
        @dataclass
        class _ReadNextState:
            class _LoopException(Exception):
                def __init__(self, task: str, *args, **kwargs):
                    self.task: str = task
                    super().__init__(*args, **kwargs)
            class ContinueException(_LoopException): pass
            class ErrorException(_LoopException):
                def __init__(self, task: str, e: Exception, *args, **kwargs):
                    self.error: Exception = e
                    super().__init__(task, *args, **kwargs)
            class ReturnException(_LoopException):
                def __init__(self, task: str, data: dict, meta: dict, *args, **kwargs):
                    self.data: dict = data
                    self.meta: dict = meta
                    super().__init__(task, *args, **kwargs)
            class EndIterationException(_LoopException): pass


            reader: MsgReader
            n: int = 0
            log: List[str] = field(default_factory=list)
            start_time: datetime = field(default_factory=datetime.now)
            error: Exception = None

            def async_shield(func):
                @functools.wraps(func)
                async def wrapper(*args, **kwargs):
                    try:
                        result = await func(*args, **kwargs)
                    except _ReadNextState._LoopException:
                        raise
                    except Exception as e:
                        raise _ReadNextState.ErrorException(func.__name__, e)
                    return result
                return wrapper

            @async_shield
            async def pop_msg(self) -> None:
                if len(self.reader.messages) > 0:
                    bmsg = self.reader.messages.popleft()
                    try: # nonessential
                        await bmsg.ack()
                    except Exception as e:
                        log.warning(self.fmt(f"Error acking message: {e}"))
                    data, meta = self.reader.messenger.unpack_nats_msg(bmsg)
                    meta['receive_mode'] = 'pull'
                    try: # nonessential
                        self.reader.messenger.log_msg_trace(data, meta, f"SUB PULL iteration from {self.reader.subject}")
                        self.reader.last_seq = meta['nats']['seq']
                        if len(self.reader.messages) == 0:
                            self.reader._emptied.set()
                    except Exception as e:
                        log.warning(self.fmt(f"Error unpacking message: {e}"))
                    raise  self.ReturnException('pop', data, meta)
            @async_shield
            async def ensure_open(self) -> None:
                if not self.reader.is_open:
                    if self.reader._expect_beeing_open:
                        log.warning(self.fmt("The reader is not open, but once it has been, trying to reopen"))
                    await self.reader.open()
                    if self.reader.is_open:
                        self.reader._expect_beeing_open = True
                        raise self.ContinueException('open')
                    else:
                        raise Exception('Opened, but still close')
            @async_shield
            async def ensure_not_stopped(self) -> None:
                if self.reader._stop.is_set():
                    await self.reader.close()
                    raise self.EndIterationException('stop')
            @async_shield
            async def ensure_consumer(self) -> None:
                if self.error is not None:
                    try:
                        ci = await self.reader.pull_subscription.consumer_info()
                        print(ci)
                    except nats.js.errors.NotFoundError:
                        log.warning(self.fmt("Consumer has gone, trying to recreate it"))
                        await self.reader._reopen()
                        log.info(self.fmt(f"Consumer re-opened"))
                        raise self.ContinueException('reopen')
            @async_shield
            async def read_batch(self) -> None:
                if len(self.reader.messages) == 0:
                    timeout = min(0.1 + self.n/10.0, 5.0)
                    # batch = 1 if self.error is not None else 100 # recover slowly
                    log.debug(self.fmt(f"Pulling {self.reader.batch} messages with timeout {timeout}s"))
                    new_msgs = await self.reader.fetch_available(batch=self.reader.batch, timeout=timeout)
                    log.debug(self.fmt(f"Pulled {len(new_msgs)} messages"))
                    self.reader.messages.extend(new_msgs)
                    raise self.ContinueException('read')

            def fmt(self, msg: str) -> str:
                return f"({self.n}){self.reader} {msg} elapsed: {(datetime.now() - self.start_time).total_seconds():.1f}s hist: {':'.join(self.log)}"
            def logput(self, msg: str) -> None:
                if len(self.log) > 0 and self.log[-1] == msg:
                    return
                if len(self.log) > 15:
                    self.log = self.log[-15:]
                    self.log[0] = '...'
                self.log.append(msg)

        st = _ReadNextState(self)
        # The loop which tries hard to get some data
        while True:
            try:
                # 0. Do we have some data to return already?
                await st.pop_msg()
                # 1. Check if iteration have been stopped externally
                await st.ensure_not_stopped()
                # 2. Is it at least open?
                await st.ensure_open()
                # 3. Check consumer, maybe ephemeral consumer is gone
                await st.ensure_consumer()
                # 4. Pull batch of  messages
                await st.read_batch()
            except st.ContinueException as e:  # one of the method did something
                st.logput(f'{e.task}-ok')
                log.debug(st.fmt(f"continue after: {e.task}"))
            except st.ReturnException as e:  # we have data to return
                st.logput(f'{e.task}-ret')
                if st.error is not None:
                    log.info(st.fmt(f"recovered"))
                    st.error = None
                log.debug(st.fmt(f"data returned"))
                return e.data, e.meta
            except st.EndIterationException as e:
                st.logput(f'{e.task}-fin')
                log.info(st.fmt(f"iteration stoped on request"))
                raise MessengerReaderStopped
            except st.ErrorException as e:  # some error
                st.logput(f'{e.task}-err')
                st.error = e.error
                match self.error_behavior:
                    case 'RAISE':
                        log.error(st.fmt(f"raising read_next error:  {e.error}"))
                        raise e.error
                    case 'FINISH':
                        log.error(st.fmt(f"finishing iteration on error: {e.error}"))
                        raise MessengerReaderStopped
                    case 'WAIT':
                        wait_time = min(0.2 + st.n/5.0, 15.0)
                        log.warning(st.fmt(f"read_next error, (retry in {wait_time:.1f}s): {e.error}"))
                        await asyncio.sleep(wait_time)
                    case _:  # should not be reached
                        log.error(st.fmt(f"Invalid on_connection_close value {self.error_behavior}"))
                        exit(-1) # this is not i/o error but programming error
            except Exception as e:# should never happen
                log.error(st.fmt(f"unhandled exception {e}"))
                exit(-1) # this is not i/o error but programming error
            st.n += 1
        # end while


    async def fetch_available(self, batch=10, timeout = 0.1):
        """Fetch only immediately available messages without blocking

        Path on lack of functionality of JetStreamContext.PullSubscription.fetch
        By default uses only very short timeout to account for network latency
        """
        import json

        pull_subscription = self.pull_subscription
        # Get from internal queue first
        msgs = []
        needed = batch
        queue = pull_subscription._sub._pending_queue

        # First get messages from the internal queue
        while not queue.empty() and needed > 0:
            try:
                msg = queue.get_nowait()
                pull_subscription._sub._pending_size -= len(msg.data)
                status = JetStreamContext.is_status_msg(msg)
                if not status:  # Skip status messages
                    msgs.append(msg)
                    needed -= 1
            except Exception:
                pass

        # If we already have enough messages, return early
        if needed == 0:
            return msgs

        # Make one no_wait request to get immediately available messages
        next_req = {"batch": needed, "no_wait": True}
        await pull_subscription._nc.publish(
            pull_subscription._nms,
            json.dumps(next_req).encode(),
            pull_subscription._deliver,
        )

        start_time = time.monotonic()

        # Process the response with a very short timeout
        while needed > 0:
            deadline = timeout - (time.monotonic() - start_time)
            if deadline <= 0:
                break

            try:
                msg = await pull_subscription._sub.next_msg(timeout=deadline)
                status = JetStreamContext.is_status_msg(msg)
                if not status:
                    msgs.append(msg)
                    needed -= 1
                elif status == "404":  # NO_MESSAGES
                    break
            except asyncio.TimeoutError:
                break

        return msgs

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
        ret = await self.connection.js.pull_subscribe(self.subject,
                                                       durable=consumer_conf.durable_name,
                                                       config=consumer_conf)
        ci = await ret.consumer_info()
        return ret

    async def _reopen(self) -> None:
        try:
            await self.pull_subscription.unsubscribe()
        except Exception:
            pass
        consumer_conf = await self._create_consumer_cfg()
        # set policy for 'new messages' for BY_START_SEQUENCE if any message was received
        if self.last_seq is not None:
            consumer_conf.deliver_policy = DeliverPolicy.BY_START_SEQUENCE
            consumer_conf.opt_start_time = None
            consumer_conf.opt_start_seq = self.last_seq + 1
        self.pull_subscription = await self._create_pull_subscribtion(consumer_conf=consumer_conf)



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
