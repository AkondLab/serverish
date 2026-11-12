from __future__ import annotations

import logging
import asyncio
from asyncio import Event
from collections import deque
from datetime import datetime, timezone
import time
from dataclasses import dataclass, field
import functools


import nats.errors
import nats.js.errors
import param
from nats.js import JetStreamContext
from nats.js.api import DeliverPolicy, ConsumerConfig

from serverish.base import wait_for_psce
from serverish.base.exceptions import (MessengerReaderStopped, MessengerReaderConfigError,
                                       MessengerNotConnected, MessengerJetStreamError,
                                       MessengerReaderAlreadyOpen, MessengerRequestTimeout)
from serverish.base.fifoset import FifoSet
from serverish.connection.nats_errors import nats_errors_translated, translate_nats_error
from serverish.messenger import Messenger
from serverish.messenger.messenger import MsgDriver, subject_family

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
        family (bool): read the subject family - the subject itself and all subjects extending it with
            further tokens (``a.b`` and ``a.b.>``), but not its siblings. `meta['nats']['subject']`
            tells the actual subject of each message
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

    family = param.Boolean(default=False,
                           doc="Read the subject family: the subject and all subjects extending it with "
                               "further tokens")

    def __init__(self, subject, parent = None,
                 deliver_policy = 'all',
                 opt_start_time = None,
                 consumer_cfg=None,
                 **kwargs) -> None:
        if parent is None:
            parent = Messenger()
        if kwargs.get('family'):
            subject_family(subject)  # validates: fails early for a subject which cannot have a family
        consumer_cfg_defaults = {
            'inactive_threshold': 300  # 5 minutes - allows ample time for health checks to detect and recover
        }
        if consumer_cfg is not None:
            consumer_cfg_defaults.update(consumer_cfg)
        # pull request size == read-ahead buffer bound; becomes BatchPolicy (docs/design/POLICIES.md)
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
        # Health monitoring fields
        self._message_count: int = 0
        self._last_message_time: float | None = None
        self._reconnect_count: int = 0
        self._last_error: Exception | None = None
        self._last_health_check_time: float | None = None
        super().__init__(subject=subject, parent=parent,
                         deliver_policy=deliver_policy, opt_start_time=opt_start_time, consumer_cfg=consumer_cfg_defaults,
                         **kwargs)
        # Validate deliver_policy ↔ start-marker consistency up front so
        # callers get a clear error at construction time rather than an
        # opaque NATS 400 error buried inside the read loop.
        if self.deliver_policy == 'by_start_time' and self.opt_start_time is None:
            raise ValueError(
                "deliver_policy='by_start_time' requires opt_start_time to be set"
            )
        if self.deliver_policy == 'by_start_sequence' and self.consumer_cfg.get('opt_start_seq') is None:
            raise ValueError(
                "deliver_policy='by_start_sequence' requires opt_start_seq to be set in consumer_cfg"
            )
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

        Raises:
            MessengerReaderStopped: the reader has been stopped (or finished, see `nowait`)
            MessengerReaderConfigError: the server rejected the reader configuration, retrying is pointless
            MessengerTransportError: (`error_behavior='RAISE'` only) failure of the underlying NATS client
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
            last_consumer_check: float = field(default_factory=time.monotonic)
            consumer_check_interval: float = 10.0  # Check consumer health every 10 seconds

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
                        # Update health monitoring stats
                        self.reader._message_count += 1
                        self.reader._last_message_time = time.monotonic()
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
                # Proactive health check: verify consumer exists periodically (every 10s)
                # This prevents silent failures when ephemeral consumers expire
                now = time.monotonic()
                should_check = (
                    self.error is not None or  # Always check after errors
                    (now - self.last_consumer_check) > self.consumer_check_interval  # Periodic check
                )

                if should_check:
                    self.last_consumer_check = now
                    self.reader._last_health_check_time = now
                    try:
                        ci = await self.reader.pull_subscription.consumer_info()
                        # Consumer exists and is accessible
                        log.debug(self.fmt(f"Consumer health check OK: {ci.name}"))
                    except nats.js.errors.NotFoundError:
                        log.warning(self.fmt("Consumer has gone (detected by health check), recreating"))
                        await self.reader._reopen()
                        log.info(self.fmt(f"Consumer re-opened after health check detected loss"))
                        raise self.ContinueException('reopen')
                    except Exception as e:
                        log.warning(self.fmt(f"Error during consumer health check: {e}, will try to recreate"))
                        await self.reader._reopen()
                        log.info(self.fmt(f"Consumer re-opened after health check error"))
                        raise self.ContinueException('reopen')

            @async_shield
            async def check_reconnect_needed(self) -> None:
                """Check if NATS reconnection occurred and recreate consumer if needed"""
                if self.reader._reconnect_needed.is_set():
                    self.reader._reconnect_needed.clear()
                    log.info(self.fmt("NATS reconnection detected, recreating consumer"))
                    await self.reader._reopen()
                    log.info(self.fmt("Consumer recreated after NATS reconnection"))
                    raise self.ContinueException('reconnect')

            @async_shield
            async def read_batch(self) -> None:
                if len(self.reader.messages) == 0:
                    # Check for reconnection before fetching
                    await self.check_reconnect_needed()

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

                        # Non-nowait mode: wait for NEW messages to arrive.
                        # Break into shorter intervals so the reconnect-needed
                        # check at the top of the loop runs often enough that
                        # low-rate subjects don't sit on a stale subscription
                        # for tens of seconds after a NATS reconnect.
                        blocking_interval = 2.0
                        max_wait_cycles = 50  # Total wait: 50 * 2s = 100s (unchanged)

                        for cycle in range(max_wait_cycles):
                            # Check for reconnection before each wait cycle
                            if self.reader._reconnect_needed.is_set():
                                self.reader._reconnect_needed.clear()
                                log.info(self.fmt("NATS reconnection detected during wait, recreating consumer"))
                                await self.reader._reopen()
                                raise self.ContinueException('reconnect_during_wait')

                            log.debug(self.fmt(f"Waiting for messages (cycle {cycle + 1}/{max_wait_cycles}, {blocking_interval}s)"))
                            try:
                                new_msgs = await self.reader.pull_subscription.fetch(1, timeout=blocking_interval)
                                if new_msgs:
                                    log.debug(self.fmt(f"Received {len(new_msgs)} new message(s)"))
                                    break
                            except asyncio.TimeoutError:
                                # Timeout is normal - verify consumer still exists before next cycle
                                try:
                                    await self.reader.pull_subscription.consumer_info()
                                except nats.js.errors.NotFoundError:
                                    log.warning(self.fmt("Consumer expired during wait, recreating"))
                                    await self.reader._reopen()
                                    raise self.ContinueException('consumer_expired_during_wait')
                                except Exception as e:
                                    log.warning(self.fmt(f"Error checking consumer during wait: {e}"))
                                    # Continue to next cycle, will be handled by health check
                            except Exception as e:
                                log.warning(self.fmt(f"Error waiting for messages: {e}"))
                                break  # Exit loop on other errors

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
                self._last_error = e.error  # Track for health monitoring
                # Fatal NATS API errors (4xx): the same request will never
                # succeed — stop retrying immediately and surface a clear
                # MessengerReaderConfigError to the caller.
                # (open() reports client errors translated, the original is its cause)
                if isinstance(e.error, MessengerReaderConfigError):  # already diagnosed as fatal
                    log.error(st.fmt(f"fatal reader configuration error (not retrying): {e.error}"))
                    self._stop.set()
                    raise e.error
                cause = e.error.__cause__ if isinstance(e.error, MessengerJetStreamError) else e.error
                if isinstance(cause, (nats.js.errors.BadRequestError,
                                      nats.js.errors.NotFoundError)):
                    log.error(st.fmt(f"fatal NATS error (not retrying): {e.error}"))
                    self._stop.set()
                    raise MessengerReaderConfigError(
                        f"Fatal NATS error on {self.subject}: {e.error}"
                    ) from e.error
                match self.error_behavior:
                    case 'RAISE':
                        log.error(st.fmt(f"raising read_next error:  {e.error}"))
                        error = translate_nats_error(e.error, f"Reading from '{self.subject}' failed")
                        if error is e.error:
                            raise e.error
                        raise error from e.error
                    case 'FINISH':
                        log.error(st.fmt(f"finishing iteration on error: {e.error}"))
                        raise MessengerReaderStopped
                    case 'WAIT':
                        wait_time = min(0.2 + st.n/5.0, 15.0)
                        log.warning(st.fmt(f"read_next error, (retry in {wait_time:.1f}s): {e.error}"))
                        await asyncio.sleep(wait_time)
                    case _:  # should not be reached
                        log.error(st.fmt(f"Invalid on_connection_close value {self.error_behavior}. "
                                         f"Report this as a bug to the serverish maintainers!"))
                        raise RuntimeError(f"Invalid on_connection_close value {self.error_behavior}. Report this as a bug to the serverish maintainers!")
            except Exception as e:# should never happen
                log.error(st.fmt(f"Unhandled exception {e}. Report this as a bug to the serverish maintainers!"))
                raise RuntimeError(f"Unhandled exception {e}. Report this as a bug to the serverish maintainers!")
            st.n += 1
        # end while


    async def fetch_available(self, batch=10, timeout=2.0):
        """[NATS fix] Fetch only immediately available messages without blocking for new messages

        Uses no_wait=True protocol:
        - Server sends available messages immediately
        - Server sends 404 status if no messages exist

        End-of-data is detected deterministically from the `num_pending`
        counter the server stamps on every delivered message - the call
        returns as soon as the server confirms there is nothing more (or the
        batch is full), with no fixed waiting anywhere on the happy path.

        Args:
            batch: Maximum number of messages to fetch
            timeout: Network-failure backstop (default 2s). Never consumed on
                     the happy path - NOT a "wait for messages" timeout

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

        # Wait for server response (messages and/or closing status).
        #
        # Deterministic end-of-data: every JetStream delivery is stamped with
        # `num_pending` — the server-side count of messages still remaining
        # for this consumer *after* that message (correct also for filtered
        # and last_per_subject consumers). The loop therefore ends without
        # any arbitrary grace period:
        #   - a message arrives with num_pending == 0 → nothing left, return,
        #   - batch filled while num_pending > 0 → return, the caller pulls again,
        #   - 404 (empty subject) / 408 (partial-batch closure) → server-confirmed end.
        # `timeout` is a network-failure backstop only; no happy path consumes
        # it, so the call is as fast as the server and the network allow.
        start_time = time.monotonic()

        def log_backstop():
            if msgs:
                log.warning(f"{self} No end-of-data signal within {timeout}s backstop, "
                            f"returning {len(msgs)} messages (no num_pending metadata? network issue?)")
            else:
                log.warning(f"{self} Timeout after {timeout}s waiting for server response (network issue?)")

        while True:
            remaining = timeout - (time.monotonic() - start_time)
            if remaining <= 0:
                log_backstop()
                break

            try:
                msg = await asyncio.wait_for(queue.get(), timeout=remaining)
            except asyncio.TimeoutError:
                log_backstop()
                break
            except Exception as e:
                log.warning(f"{self} Error in fetch_available: {e}")
                break
            pull_subscription._sub._pending_size -= len(msg.data)
            queue.task_done()

            status = JetStreamContext.is_status_msg(msg)
            if status == "404":
                # Server says: no messages available at all
                log.debug(f"{self} Server confirmed no messages (404 status)")
                break
            elif status == "408":
                # Request Timeout - server closed the request after a partial batch
                log.debug(f"{self} Server closed partial batch (408 status)")
                break
            elif status:
                # Other status message (shouldn't happen with pull, but handle it)
                log.debug(f"{self} Received status: {status}")
                continue

            # Real message
            msgs.append(msg)
            needed -= 1

            num_pending = self._num_pending(msg)
            if num_pending == 0:
                # Server-confirmed end of data - no reason to wait for the
                # closing status (which may trail by a network round trip,
                # or never come for an exact-batch response)
                log.debug(f"{self} Server reports num_pending=0, returning {len(msgs)} messages")
                break
            if needed == 0:
                # Batch full and the server has more - return immediately,
                # the caller decides whether to pull again
                log.debug(f"{self} Batch of {len(msgs)} filled, "
                          f"server reports {num_pending} message(s) still pending")
                break
            # num_pending unknown (no JS metadata): keep reading until a
            # closing status arrives or the backstop hits (degraded mode)

        return msgs

    @staticmethod
    def _num_pending(msg: Msg) -> int | None:
        """Returns server-side `num_pending` from JetStream message metadata

        `num_pending` is the number of messages still remaining for this
        consumer after the given message. Returns None if the metadata is
        unavailable (non-JetStream message).
        """
        try:
            return msg.metadata.num_pending
        except Exception:
            return None

    async def open(self) -> None:
        """Opens the reader: creates the JetStream consumer and subscribes to it

        Raises:
            MessengerReaderAlreadyOpen: the reader is open already
            MessengerNotConnected: no connection to subscribe through
            MessengerRequestTimeout: the server did not create the consumer in time
            MessengerTransportError: any other failure of the underlying NATS client
        """
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
        js = self.connection.js
        with nats_errors_translated(f"Creating consumer for '{self.subject}' failed"):
            try:
                subject, stream, filters = self.subject, None, None
                if self.family:
                    # Subject family: one consumer, two filters - no single pattern matches
                    # both 'a.b' and 'a.b.c' ('a.b.>' requires at least one more token)
                    stream, filters = await self._resolve_family()
                    if len(filters) == 1:
                        subject = filters[0]  # only this part of the family is persistent
                    elif filters:
                        consumer_conf.filter_subjects = filters
                ret = await js.pull_subscribe(subject,
                                              durable=consumer_conf.durable_name,
                                              stream=stream,
                                              config=consumer_conf)
            except nats.errors.TimeoutError as e:
                raise MessengerRequestTimeout(
                    f"Creating consumer for '{self.subject}' timed out: {e}") from e
            ci = await ret.consumer_info()
            if filters:
                await self._verify_family_filters(ret, ci, filters)
        return ret

    async def _resolve_family(self) -> tuple[str | None, list[str]]:
        """Finds where the subject family is stored and which of its patterns are

        The family may be persistent as a whole or in part - e.g. only the keyed
        subjects ``a.b.>`` while the bare ``a.b`` is not. Looking the storage up
        by the bare subject alone would miss the latter case.

        Returns:
            the stream storing the family and the family patterns it covers;
            ``(None, [])`` if no part of the family is persistent (the caller
            then fails on the bare subject, as a plain reader would)

        Raises:
            MessengerReaderConfigError: the family is split between two storages -
                no single reader can deliver it whole, and a part of it would be a silent lie
        """
        js = self.connection.js
        stream: str | None = None
        covered: list[str] = []
        for pattern in subject_family(self.subject):
            try:
                found = await js.find_stream_name_by_subject(pattern)
            except nats.js.errors.NotFoundError:
                continue
            if stream is not None and found != stream:
                raise MessengerReaderConfigError(
                    f"The subject family of '{self.subject}' is stored in two places ('{covered[0]}' in "
                    f"'{stream}', '{pattern}' in '{found}') and cannot be read as one. "
                    f"Store the family together, or read its parts with separate readers")
            stream = found
            covered.append(pattern)
        return stream, covered

    async def _verify_family_filters(self, subscription, consumer_info, filters: list[str]) -> None:
        """Makes sure the server enforces the family filters, removes the consumer otherwise

        A server which does not know multiple subject filters (NATS < 2.10)
        ignores them and would deliver every message of the storage, not the family.

        Raises:
            MessengerReaderConfigError: the server did not apply the filters
        """
        config = consumer_info.config
        enforced = config.filter_subjects or ([config.filter_subject] if config.filter_subject else [])
        if set(enforced) == set(filters):
            return
        try:
            await subscription.unsubscribe()
            await self.connection.js.delete_consumer(stream=consumer_info.stream_name, consumer=consumer_info.name)
        except Exception as e:
            log.warning(f"{self} could not remove the unfiltered consumer: {e}")
        raise MessengerReaderConfigError(
            f"The server does not filter the subject family of '{self.subject}' (asked for {filters}, "
            f"got {enforced or 'no filter'}). Subject families on persistent subjects need NATS server 2.10 or newer")

    async def _reopen(self) -> None:
        """Reopens the pull subscription with retry logic and better error handling"""
        self._reconnect_count += 1
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Clean up existing subscription. `unsubscribe()` only detaches the
                # local handle; the JetStream consumer must be deleted explicitly
                # or it leaks server-side across every reconnect (one orphan per
                # reopen, accumulating until the stream's consumer cap or the
                # process restarts).
                if self.pull_subscription is not None:
                    try:
                        ci = await self.pull_subscription.consumer_info()
                    except Exception as e:
                        log.debug(f"Error fetching consumer_info during reopen attempt {attempt + 1}: {e}")
                        ci = None
                    try:
                        await self.pull_subscription.unsubscribe()
                    except Exception as e:
                        log.debug(f"Error unsubscribing during reopen attempt {attempt + 1}: {e}")
                    if ci is not None:
                        try:
                            await self.connection.js.delete_consumer(stream=ci.stream_name, consumer=ci.name)
                        except nats.js.errors.NotFoundError:
                            pass  # already gone, nothing to delete
                        except Exception as e:
                            log.debug(f"Error deleting old consumer during reopen attempt {attempt + 1}: {e}")
                
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
        reader_params = ['nowait', 'error_behavior', 'on_missed_messages', 'family']
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

                ts_utc = self.opt_start_time.astimezone(tz=timezone.utc)
                # Workaround for nats-py round-trip bug: ``Base._to_utc_iso``
                # strips ``.000000`` on whole-second values, but
                # ``Base._parse_utc_iso`` unconditionally splits on '.', so the
                # consumer-info echo for a zero-microsecond ``opt_start_time``
                # raises ``ValueError: not enough values to unpack``. Bump
                # microseconds to 1 so the server always echoes a fractional
                # part. Harmless: 1 µs is well below JetStream's storage
                # resolution and the start-time semantics.
                if ts_utc.microsecond == 0:
                    ts_utc = ts_utc.replace(microsecond=1)
                cfg['opt_start_time'] = ts_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

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

    @property
    def health_status(self) -> dict:
        """Returns current health status of the reader for monitoring

        Returns:
            dict with health information:
                - is_open: Whether the reader is currently open
                - subject: The subject being read
                - messages_received: Total count of messages received
                - last_message_time: Timestamp of last message (monotonic time) or None
                - last_message_ago: Seconds since last message or None if no messages yet
                - reconnect_count: Number of times the consumer was recreated
                - last_error: String representation of last error or None
                - last_health_check_time: Timestamp of last health check or None
                - pending_messages: Number of messages in local pending queue
                - pending_bytes: Bytes in local pending queue
                - connection_slow_consumers: Slow consumer count from connection (for diagnostics)
        """
        last_msg_ago = None
        if self._last_message_time is not None:
            last_msg_ago = time.monotonic() - self._last_message_time

        # Get pending stats from pull subscription if available
        pending_messages = 0
        pending_bytes = 0
        if self.pull_subscription is not None:
            try:
                pending_messages = self.pull_subscription._sub._pending_queue.qsize()
                pending_bytes = self.pull_subscription._sub._pending_size
            except (AttributeError, Exception):
                pass  # Subscription internals not available

        # Get slow consumer count from connection for diagnostics
        connection_slow_consumers = 0
        try:
            connection_slow_consumers = self.connection._slow_consumer_count
        except (AttributeError, Exception):
            pass

        return {
            'is_open': self.is_open,
            'subject': self.subject,
            'messages_received': self._message_count,
            'last_message_time': self._last_message_time,
            'last_message_ago': last_msg_ago,
            'reconnect_count': self._reconnect_count,
            'last_error': str(self._last_error) if self._last_error else None,
            'last_health_check_time': self._last_health_check_time,
            'pending_messages': pending_messages,
            'pending_bytes': pending_bytes,
            'connection_slow_consumers': connection_slow_consumers,
        }

    async def check_consumer_exists(self) -> bool:
        """Check if consumer still exists without raising

        Returns:
            True if consumer exists, False otherwise
        """
        if self.pull_subscription is None:
            return False
        try:
            await self.pull_subscription.consumer_info()
            return True
        except Exception:
            return False

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
