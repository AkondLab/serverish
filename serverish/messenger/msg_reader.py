from __future__ import annotations

import logging
import asyncio
from asyncio import Event
from collections import deque
from datetime import datetime, timezone
import time
from dataclasses import dataclass, field
import functools
from uuid import uuid4


import nats.errors
import param
from nats.js import JetStreamContext
from nats.js.api import DeliverPolicy, ConsumerConfig

from serverish.base import wait_for_psce
from serverish.base.exceptions import (MessengerReaderStopped, MessengerNotConnected,
                                       MessengerReaderAlreadyOpen, MessengerRequestTimeout)
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

    Args:
        subject (str): subject to subscribe to
        parent (Messenger): Messenger instance to use, if None, will use default Messenger()
        deliver_policy (str): deliver policy, one of 'all', 'last', 'new', 'by_start_sequence', 'by_start_time', 'last_per_subject'
        opt_start_time (datetime): start time for 'by_start_time' deliver policy
        consumer_cfg (dict): additional consumer configuration, see nats.js.api.ConsumerConfig
        nowait (bool): if True, read_next will return immediately if no messages are available and will finish iteration
        error_behavior (str): on serious error (e.g. disconnection), one of 'RAISE', 'FINISH', 'WAIT'
        on_missed_messages (str): on missed messages (e.g. during broken connection), one of 'SKIP', 'REPLAY'

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
    nowait = param.Boolean(default=False,
                            doc="If True, read_next will return immediately if no messages are available and will "
                                "finish iteration")

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
                    try: # nonessential  #TODO: implement delayed acking / no acking as na option
                        await bmsg.ack()
                    except Exception as e:
                        log.warning(self.fmt(f"Error acking message: {e}"))
                        # Not essential - we can continue without acking

                    try:
                        data, meta = self.reader.messenger.unpack_nats_msg(bmsg)
                    except Exception as e:
                        log.warning(self.fmt(f"Error unpacking message: {e}"))
                        # Mesaage malformed - non recoverable error (for this message)
                        raise self.ErrorException('pop', e)

                    try:
                        if self.reader.last_seq is not None and meta['nats']['seq'] <= self.reader.last_seq: # dupes can happen on reopen
                            log.info(self.fmt(f"Skipping duplicated message seq={meta['nats']['seq']}, last_seq={self.reader.last_seq}"))
                            raise self.ContinueException('pop')
                    except self.ContinueException:
                        # Duplicate removed from queue - recovered from a problem
                        raise
                    except Exception as e:
                        # Error checking sequence, can be ignored
                        log.warning(self.fmt(f"Error checking seq: {e}"))
                    meta['receive_mode'] = 'pull'
                    try: # nonessential
                        self.reader.messenger.log_msg_trace(data, meta, f"SUB PULL iteration from {self.reader.subject}")
                        self.reader.last_seq = meta['nats']['seq']
                        if len(self.reader.messages) == 0:
                            self.reader._emptied.set()
                    except Exception as e:
                        # Error in logging, can be ignored
                        log.warning(self.fmt(f"Error posprocessing message: {e}"))
                    # log.info(self.fmt(f"Returning message seq={meta['nats']['seq']}, last_seq={self.reader.last_seq}"))
                    raise  self.ReturnException('pop', data, meta)

            @async_shield
            async def ensure_open(self) -> None:
                if not self.reader.is_open:
                    if self.reader._expect_beeing_open:
                        log.warning(self.fmt("The reader is not open, but once it has been, trying to reopen"))
                    await self.reader.open()  # exceptions handled by the async_shield decorator
                    if self.reader.is_open:
                        self.reader._expect_beeing_open = True
                        # Recover from a problem
                        raise self.ContinueException('open')
                    else:
                        raise Exception('Opened, but still close') # exception handled by the async_shield decorator

            @async_shield
            async def ensure_not_stopped(self) -> None:
                if self.reader._stop.is_set():
                    try:
                        await self.reader.close()
                    except Exception as e:
                        # Error in closing can be ignored we are finishing iteration anyway
                        log.warning(self.fmt(f"Error closing reader: {e}"))
                    raise self.EndIterationException('stop')

            @async_shield
            async def ensure_consumer(self) -> None:
                if self.error is not None:
                    try:
                        ci = await self.reader.pull_subscription.consumer_info()
                        # Consumer exists and is accessible
                        log.debug(self.fmt(f"Consumer check OK: {ci.name}"))
                    except nats.js.errors.NotFoundError:
                        log.warning(self.fmt("Consumer has gone, trying to recreate it"))
                        await self.reader._reopen()
                        log.info(self.fmt(f"Consumer re-opened"))
                        raise self.ContinueException('reopen')
                    except Exception as e:
                        log.warning(self.fmt(f"Error checking consumer, will try to recreate anyway: {e}"))
                        await self.reader._reopen()
                        log.info(self.fmt(f"Consumer looks like re-opened after error"))
                        raise self.ContinueException('reopen')

            @async_shield
            async def read_batch(self) -> None:
                if len(self.reader.messages) == 0:
                    # For fetch_available, timeout is network latency budget (not message wait time)
                    # Server responds immediately with no_wait=True, we just account for slow networks
                    fetch_timeout = 2.0  # Reduced - should be quick with no_wait
                    log.debug(self.fmt(f"Pulling {self.reader.batch} messages with timeout {fetch_timeout}s"))
                    new_msgs = await self.reader.fetch_available(batch=self.reader.batch, timeout=fetch_timeout)
                    log.debug(self.fmt(f"Pulled {len(new_msgs)} messages"))

                    # If no messages were available (got 404 from server or timeout), decide what to do
                    if len(new_msgs) == 0:
                        if self.reader.nowait:
                            # Server confirmed no messages - finish iteration
                            log.debug(self.fmt(f"No messages available, finishing due to nowait"))
                            raise self.EndIterationException('nowait')

                        # Non-nowait mode: wait for NEW messages to arrive
                        blocking_timeout = 100.0
                        log.debug(self.fmt(f"No messages currently available, waiting for new messages (timeout {blocking_timeout}s)"))

                        # Use the regular fetch operation which waits for messages to arrive
                        try:
                            new_msgs = await self.reader.pull_subscription.fetch(1, timeout=blocking_timeout)
                            log.debug(self.fmt(f"Received {len(new_msgs)} new message(s)"))
                        except asyncio.TimeoutError:
                            log.debug(self.fmt(f"No new messages arrived within {blocking_timeout}s"))
                        except Exception as e:
                            log.warning(self.fmt(f"Error waiting for messages: {e}"))

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
                # If the step method:
                #  - retuns data                       - it will raiseReturnException
                #  - corrected something               - it will raise ContinueException
                #  - wants stop iteration              - it will raise EndIterationException
                #  - encounters an unrecoverable error - it will raise ErrorException
                #  - did not do anything               - it will return and next step will be called
                # Those mthod should not raise any other exceptions, if they do, it is a bug,
                # this is enforced by @async_shield (conversion of nay exception to ErrorException)

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
                        log.error(st.fmt(f"Invalid on_connection_close value {self.error_behavior}. "
                                         f"Raport this as a bug to the serverish maintainers!"))
                        exit(-107) # this is not i/o error but programming error
            except Exception as e:# should never happen
                log.error(st.fmt(f"Unhandled exception {e}. Raport this as a bug to the serverish maintainers!"))
                exit(-108) # this is not i/o error but programming error
            st.n += 1
        # end while


    async def fetch_available(self, batch=10, timeout=2.0):
        """[NATS fix] Fetch only immediately available messages without blocking for new messages

        Uses no_wait=True protocol:
        - Server sends available messages immediately
        - Server sends 404 status if no messages exist
        - We wait for server response (network latency) but not for new messages to arrive

        Args:
            batch: Maximum number of messages to fetch
            timeout: Network latency budget (default 2s for local/fast networks)
                     NOT a "wait for messages" timeout - server won't wait

        Returns:
            List of messages that were immediately available on server
        """
        import json

        pull_subscription = self.pull_subscription
        msgs = []
        needed = batch
        queue = pull_subscription._sub._pending_queue

        # First, drain messages already in local queue
        while not queue.empty() and needed > 0:
            try:
                msg = queue.get_nowait()
                pull_subscription._sub._pending_size -= len(msg.data)
                status = JetStreamContext.is_status_msg(msg)
                if not status:  # Skip status messages
                    msgs.append(msg)
                    needed -= 1
                queue.task_done()
            except Exception:
                pass

        # If we got enough from queue, return immediately
        if needed == 0:
            return msgs

        # Send pull request: no_wait=True means server responds immediately
        next_req = {"batch": needed, "no_wait": True}
        await pull_subscription._nc.publish(
            pull_subscription._nms,
            json.dumps(next_req).encode(),
            pull_subscription._deliver,
        )

        # Wait for server response (messages and/or 404 status)
        # The server will respond quickly because no_wait=True
        # We just need to account for network latency
        start_time = time.monotonic()
        got_404_status = False
        got_any_message = False

        while not got_404_status:
            deadline = timeout - (time.monotonic() - start_time)
            if deadline <= 0:
                # Timeout - if we got some messages, that's OK (server might not send 404 after exact batch)
                # If we got nothing, this is a network problem
                if not got_any_message and len(msgs) == 0:
                    log.warning(f"{self} Timeout after {timeout}s waiting for server response (network issue?)")
                else:
                    log.debug(f"{self} Timeout after getting {len(msgs)} messages (no 404 received)")
                break

            try:
                # Wait for server to respond (messages or 404)
                msg = await asyncio.wait_for(queue.get(), timeout=deadline)
                pull_subscription._sub._pending_size -= len(msg.data)
                queue.task_done()

                status = JetStreamContext.is_status_msg(msg)

                if status == "404":
                    # Server says: no (more) messages available
                    # This is the definitive "done" signal
                    log.debug(f"{self} Server confirmed no more messages (404 status)")
                    got_404_status = True
                    break
                elif status == "408":
                    # Request Timeout - server gave us what it had but couldn't fill batch
                    # This is effectively "no more messages available right now"
                    log.debug(f"{self} Server returned partial batch (408 status - request timeout)")
                    got_404_status = True  # Treat same as 404 - no more messages available
                    break
                elif status:
                    # Other status message (shouldn't happen with pull, but handle it)
                    log.debug(f"{self} Received status: {status}")
                    continue
                else:
                    # Real message
                    msgs.append(msg)
                    got_any_message = True
                    needed -= 1

                    # If we got all requested messages, give server very short time to send 404
                    # but don't wait long - likely there are more messages
                    if needed == 0:
                        # Very short grace period for 404 status (100ms)
                        # If 404 doesn't arrive, we'll just fetch again on next iteration
                        remaining_time = timeout - (time.monotonic() - start_time)
                        if remaining_time > 0.1:
                            deadline = 0.1
                            timeout = time.monotonic() - start_time + 0.1  # Update timeout to exit on next deadline check

            except asyncio.TimeoutError:
                # Timeout after getting messages is OK - server might not send 404 if we got exact batch
                if got_any_message or len(msgs) > 0:
                    log.debug(f"{self} Got {len(msgs)} messages, no 404 status (normal for exact batch)")
                else:
                    # No messages at all - this could be empty subject or network problem
                    log.debug(f"{self} No messages or 404 within {timeout}s")
                break
            except Exception as e:
                log.warning(f"{self} Error in fetch_available: {e}")
                break

        return msgs

    async def next_msg(self, timeout: Optional[float] = None) -> Msg:
        """ [NATS fix] Fetch the next message from the subscription
        :params timeout: Time in seconds to wait for next message before timing out.
                        supports 0 for non-blocking and None for blocking indefinitely.
        :raises nats.errors.TimeoutError:

        next_msg can be used to retrieve the next message from a stream of messages using
        await syntax, this only works when not passing a callback on `subscribe`::

            sub = await nc.subscribe('hello')
            msg = await sub.next_msg(timeout=1)

        """
        ps = self.pull_subscription._sub
        if ps._conn.is_closed:
            raise errors.ConnectionClosedError

        if ps._cb:
            raise errors.Error(
                'nats: next_msg cannot be used in async subscriptions'
            )

        task_name = str(uuid4())
        try:
            if timeout == 0.0:
                future = asyncio.create_task(
                    ps._pending_queue.get_nowait()
                )
            else:
                future = asyncio.create_task(
                    asyncio.wait_for(ps._pending_queue.get(), timeout)
                )
            ps._pending_next_msgs_calls[task_name] = future
            msg = await future
        except asyncio.TimeoutError:
            if ps._conn.is_closed:
                raise errors.ConnectionClosedError
            raise errors.TimeoutError
        except asyncio.CancelledError:
            if ps._conn.is_closed:
                raise errors.ConnectionClosedError
            raise
        else:
            ps._pending_size -= len(msg.data)
            # For sync subscriptions we will consider a message
            # to be done once it has been consumed by the client
            # regardless of whether it has been processed.
            ps._pending_queue.task_done()
            return msg
        finally:
            ps._pending_next_msgs_calls.pop(task_name, None)

    async def open(self) -> None:
        if self.pull_subscription is not None:
            raise MessengerReaderAlreadyOpen("Reader already open, do not reuse MsgReader instances")

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
        if self.connection is None or self.connection.js is None:
            raise MessengerNotConnected('Cannot create pull subscription, not connected to NATS')
        try:
            ret = await self.connection.js.pull_subscribe(self.subject,
                                                           durable=consumer_conf.durable_name,
                                                           config=consumer_conf)
        except TimeoutError as e:
            raise MessengerRequestTimeout(e)
        ci = await ret.consumer_info()
        return ret

    async def _reopen(self) -> None:
        """Reopens the pull subscription with retry logic and better error handling"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Clean up existing subscription
                if self.pull_subscription is not None:
                    try:
                        await self.pull_subscription.unsubscribe()
                    except Exception as e:
                        log.debug(f"Error unsubscribing during reopen attempt {attempt + 1}: {e}")
                
                # Create new consumer configuration
                consumer_conf = await self._create_consumer_cfg()
                
                # Set policy for 'new messages' if any message was received
                if self.last_seq is not None:
                    log.info(f"Reopening {self}, from seq={self.last_seq + 1}")
                    consumer_conf.deliver_policy = DeliverPolicy.BY_START_SEQUENCE
                    consumer_conf.opt_start_time = None
                    consumer_conf.opt_start_seq = self.last_seq + 1
                
                # Create new subscription
                self.pull_subscription = await self._create_pull_subscribtion(consumer_conf=consumer_conf)
                log.info(f"Successfully reopened {self} after {attempt + 1} attempts")
                return  # Success!
                
            except Exception as e:
                if attempt == max_retries - 1:
                    log.error(f"Failed to reopen {self} after {max_retries} attempts: {e}")
                    raise
                
                wait_time = (attempt + 1) * 2  # Exponential backoff: 2s, 4s, 6s
                log.warning(f"Reopen attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                await asyncio.sleep(wait_time)



    async def _create_consumer_cfg(self) -> ConsumerConfig:
        cfg = self.consumer_cfg.copy()

        # Remove MsgReader-specific parameters that are not part of ConsumerConfig
        # These are handled by MsgReader itself, not passed to NATS
        reader_params = ['nowait', 'error_behavior', 'on_missed_messages']
        for param in reader_params:
            cfg.pop(param, None)

        # Convert the delivery policy from a string to the appropriate DeliverPolicy enum
        if self.deliver_policy is not None:
            cfg['deliver_policy'] = DeliverPolicy(self.deliver_policy)
        # from_time handling:
        if self.opt_start_time is not None:
            if isinstance(self.opt_start_time, str):
                cfg['opt_start_time'] = self.opt_start_time
            else:
                # Check if timezone is set, if not warn
                if self.opt_start_time.tzinfo is None:
                    log.warning(f"opt_start_time should have timezone information, converting to UTC: "
                                f"{self.opt_start_time.strftime('%Y-%m-%dT%H:%M:%S.%f')}"
                                f"=>{self.opt_start_time.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')}"
                                f" , use e.g. `datetime.now(tz=timezone.utc)`")

                cfg['opt_start_time'] = self.opt_start_time.astimezone(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        # Create the pull consumer configuration
        consumer_conf = ConsumerConfig(**cfg)
        return consumer_conf

    async def close(self) -> None:
        self.connection.remove_reconnect_cb(self.on_nats_reconnect)
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
