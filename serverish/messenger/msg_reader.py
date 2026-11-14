from __future__ import annotations

import logging
import asyncio
import contextlib
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

from serverish.base import BurstLog
from serverish.base.exceptions import (MessengerReaderStopped, MessengerReaderConfigError,
                                       MessengerNotConnected, MessengerJetStreamError,
                                       MessengerReaderAlreadyOpen, MessengerRequestTimeout,
                                       MessengerTransportTimeout)
from serverish.base.fifoset import FifoSet
from serverish.connection.nats_errors import (is_consumer_limit, is_missing_stream, nats_errors_translated,
                                              translate_nats_error)
from serverish.messenger import Messenger
from serverish.messenger.messenger import MsgDriver, subject_family
from serverish.messenger.policies import (UNLIMITED, BatchPolicy, DeliveryPolicy, ErrorPolicy,
                                          OnError, RetryPolicy)

log = logging.getLogger(__name__.rsplit('.')[-1])
# Connection trouble is noticed by all readers of the application at the same moment:
# the first one warns in full, the others are summed up
burst_log = BurstLog(log)

class _ReconnectNeededError(Exception):
    pass


@dataclass(eq=False)
class _ConsumerSession:
    """A JetStream consumer together with the local subscription its answers arrive on

    The two are useful only as a pair: the consumer answers pull requests to the inbox
    of the subscription. A reader holds one session, and replaces it as a whole.

    The session is judged by the only thing that matters to the reader: do the server's
    answers to its pull requests arrive. "The consumer exists on the server" proves less -
    it stays true for a session whose subscription is lost.
    """
    subscription: JetStreamContext.PullSubscription
    stream: str | None = None    # None: not known (a subscription assigned from outside)
    consumer: str | None = None
    unanswered: int = 0          # pull requests in a row the server did not answer
    gone: bool = False           # the server said the consumer does not exist any more

    def needs_replacement(self, unanswered_limit: int) -> str | None:
        """Tells why the session cannot serve the reader any more, None if it can"""
        if self.gone:
            return "the consumer is gone (expired or deleted)"
        if self.unanswered >= unanswered_limit:
            return f"{self.unanswered} pull requests in a row got no answer"
        return None

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
        error_policy (ErrorPolicy): typed alternative to error_behavior/on_missed_messages, adds RetryPolicy
            control over WAIT-mode reconnect pacing; conflicts with the flat kwargs (docs/design/POLICIES.md)
        batch_policy (BatchPolicy): pull-size control - static bound or dynamic (consumption-rate based) sizing

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

    #: Effective WAIT-mode reconnect pacing (docs/design/POLICIES.md §1.4:
    #: driver defaults live at the driver). Overridable per reader via
    #: error_policy=ErrorPolicy(retry=RetryPolicy(...)).
    retry_defaults = RetryPolicy(attempts=UNLIMITED, delay=0.2, backoff=1.5,
                                 max_delay=15.0, total_timeout=UNLIMITED)
    #: Effective batching defaults: dynamic sizing driven by the measured
    #: consumption rate. Starts tiny (2), keeps a prefetched message at most
    #: ~5 s in client memory, never requests more than 10k. Callers pin
    #: static behaviour explicitly with BatchPolicy(dynamic=False, ...).
    batch_defaults = BatchPolicy(dynamic=True, initial_batch=2,
                                 max_time_in_memory=5.0, max_batch=10_000)
    #: Pull size for static mode when the policy does not name one.
    static_batch_default = 100

    def __init__(self, subject, parent = None,
                 deliver_policy: str | DeliveryPolicy = 'all',
                 opt_start_time = None,
                 consumer_cfg=None,
                 error_policy: ErrorPolicy | None = None,
                 batch_policy: BatchPolicy | None = None,
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

        # --- policy normalization (docs/design/POLICIES.md) ---
        # `deliver_policy` is dual-typed: a string ('last', ...) is permanent
        # sugar, a DeliveryPolicy object is the full form. Satellites
        # (opt_start_time, opt_start_seq, nowait) combine only with the string.
        self.delivery: DeliveryPolicy = DeliveryPolicy.from_value(
            deliver_policy,
            opt_start_time=opt_start_time,
            opt_start_seq=consumer_cfg_defaults.get('opt_start_seq'),
            nowait=kwargs.get('nowait'))
        delivery_params = self.delivery.to_reader_params()
        deliver_policy = delivery_params['deliver_policy']
        opt_start_time = delivery_params.get('opt_start_time', opt_start_time)
        if 'opt_start_seq' in delivery_params:
            consumer_cfg_defaults['opt_start_seq'] = delivery_params['opt_start_seq']
        if 'nowait' in delivery_params:
            kwargs['nowait'] = delivery_params['nowait']

        self.errors: ErrorPolicy | None = ErrorPolicy.from_kwargs(
            error_policy,
            error_behavior=kwargs.get('error_behavior'),
            on_missed_messages=kwargs.get('on_missed_messages'))
        if self.errors is not None:
            if self.errors.on_error is not None:
                if self.errors.on_error is OnError.LOG:
                    raise ValueError("Readers do not support OnError.LOG - use RAISE, FINISH or WAIT")
                kwargs['error_behavior'] = self.errors.on_error.value
            if self.errors.on_missed is not None:
                kwargs['on_missed_messages'] = self.errors.on_missed.value
        self._retry: RetryPolicy = ((self.errors.retry if self.errors and self.errors.retry
                                     else RetryPolicy()).resolved(self.retry_defaults))

        if batch_policy is not None and not isinstance(batch_policy, BatchPolicy):
            raise ValueError(f"batch_policy must be a BatchPolicy, got {type(batch_policy).__name__}")
        self.batching: BatchPolicy = batch_policy or BatchPolicy()
        # legacy attribute: static-mode pull size / read-ahead bound
        self.batch = self.batching.max_batch or self.static_batch_default
        self._consume_stamps: deque[float] = deque(maxlen=32)  # recent consumption timestamps
        self._server_pending: int | None = None  # last num_pending reported by the server
        self._server_answered: bool = False  # did the server answer the last fetch_available()
        self._last_batch: int | None = None      # previous dynamic pull size (growth damping)
        # --- end policy normalization ---

        self.messages = deque()
        self._session: _ConsumerSession | None = None
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
        # deliver_policy ↔ start-marker consistency is validated up front by
        # DeliveryPolicy.from_value above, so callers get a clear error at
        # construction time rather than an opaque NATS 400 in the read loop.
        log.debug(f"Created {self}")


    #: Pull requests in a row without any answer after which the session is replaced.
    #: One lost answer may be a hiccup of the link; the second one, asked for after
    #: the retry pause, is a pattern.
    unanswered_limit = 2
    #: A reader waiting for messages asks the server for an immediate answer at least
    #: this often [s]: silence alone does not tell an idle subject from a lost session.
    probe_interval = 10.0

    @property
    def pull_subscription(self) -> JetStreamContext.PullSubscription | None:
        """The subscription of the current consumer session, None when the reader has none"""
        return self._session.subscription if self._session is not None else None

    @pull_subscription.setter
    def pull_subscription(self, subscription: JetStreamContext.PullSubscription | None) -> None:
        self._session = _ConsumerSession(subscription) if subscription is not None else None

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
            error_since: float = field(default_factory=time.monotonic)  # start of current error streak
            retries: int = 0  # retries scheduled in the current error streak (not loop passes)
            deadline: asyncio.Timeout | None = None  # limits the current loop pass while failing
            # cancel requests seen by the calling task when read_next() started
            cancel_requests: int = field(default_factory=lambda: asyncio.current_task().cancelling())

            def recovered(self) -> None:
                """Ends the current error streak: the reader works again, with data or without"""
                if self.error is not None:
                    log.info(self.fmt("recovered"))
                    self.error = None
                    self.retries = 0
                    if self.deadline is not None:
                        self.deadline.reschedule(None)  # the deadline was the failure's, not the reader's

            @contextlib.asynccontextmanager
            async def limited_by(self, deadline: asyncio.Timeout):
                """Makes the deadline of a loop pass known to `recovered()`, for as long as the pass lasts"""
                self.deadline = deadline
                try:
                    yield
                finally:
                    self.deadline = None

            def raise_if_cancelled(self) -> None:
                """Raises CancelledError when the calling task was cancelled during this read_next()

                A cancellation can be lost below us: a timeout and a cancel landing in the
                same loop step may surface as a plain TimeoutError (serverish#32), which the
                waiting loop takes for "no message yet" and polls on for ever. The task still
                knows it was asked to stop - honour that.
                """
                if asyncio.current_task().cancelling() > self.cancel_requests:
                    raise asyncio.CancelledError
            last_probe: float = field(default_factory=time.monotonic)  # last pull request the server was asked to answer at once

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
                        self.reader._consume_stamps.append(time.monotonic())  # feeds dynamic batch sizing
                        pending = meta['nats'].get('num_pending')
                        if pending is not None:  # the message just consumed knows best, however it was pulled
                            self.reader._server_pending = pending
                        if len(self.reader.messages) == 0 and not self.reader._server_pending:
                            # "emptied" means the SERVER has nothing more for us
                            # (num_pending == 0 or unknown), not merely that the
                            # local read-ahead drained - a pull smaller than the
                            # backlog (dynamic batching, backlog > static batch)
                            # must not signal wait_for_empty() prematurely.
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
                        burst_log.warning("Reader not open, reopening",
                                          self.fmt("The reader is not open, but once it has been, trying to reopen"))
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
            async def ensure_session(self) -> None:
                """Replaces the consumer session when it cannot serve the reader any more

                What is wrong with the session is learnt from the pull requests themselves
                (`fetch_available`), at no cost: an answer proves the whole path, from the
                consumer on the server to the queue of this reader.
                """
                reader = self.reader
                if reader._reconnect_needed.is_set():
                    reader._reconnect_needed.clear()
                    reason = "NATS reconnected"
                elif reader._session is None:
                    reason = "the reader has no consumer session"
                else:
                    reason = reader._session.needs_replacement(reader.unanswered_limit)
                if reason is None:
                    return
                burst_log.warning("Replacing the consumer session",
                                  self.fmt(f"{reason}, replacing the consumer session"))
                await reader._replace_session()
                log.info(self.fmt("Consumer session replaced"))
                raise self.ContinueException('session')

            @async_shield
            async def read_batch(self) -> None:
                if len(self.reader.messages) == 0:
                    session = self.reader._session
                    # For fetch_available, timeout is network latency budget (not message wait time)
                    # Server responds immediately with no_wait=True, we just account for slow networks
                    fetch_timeout = 2.0  # Reduced - should be quick with no_wait
                    batch = self.reader._next_batch_size()
                    log.debug(self.fmt(f"Pulling {batch} messages with timeout {fetch_timeout}s"))
                    new_msgs = await self.reader.fetch_available(batch=batch, timeout=fetch_timeout)
                    log.debug(self.fmt(f"Pulled {len(new_msgs)} messages"))
                    self.last_probe = time.monotonic()
                    if not self.reader._server_answered and not session.gone:
                        # Silence. Older servers (seen: 2.10) are also silent when the consumer
                        # is gone, newer ones say so (503) - ask, this once, which silence it is
                        session.gone = await self.reader._consumer_is_gone(session)
                    if session.gone:
                        # an ephemeral consumer expiring is ordinary life (a frozen client,
                        # a restarted server), not an error: the next pass replaces the session
                        raise self.ContinueException('gone')
                    if not self.reader._server_answered:
                        # No data and no "no data": the answers do not reach us. This is the
                        # failure, whatever the cause; the retry policy paces what follows
                        raise MessengerTransportTimeout(
                            f"The server did not answer a pull request for '{self.reader.subject}' "
                            f"within {fetch_timeout} s")
                    self.reader._last_health_check_time = self.last_probe
                    self.recovered()  # the server answers: the failure is over, even with no data

                    # If no messages were available (404 from the server), decide what to do
                    if len(new_msgs) == 0:
                        if self.reader.nowait:
                            # Server confirmed no messages - finish iteration
                            log.debug(self.fmt(f"No messages available, finishing due to nowait"))
                            raise self.EndIterationException('nowait')

                        # Non-nowait mode: wait for NEW messages to arrive.
                        # Break into shorter intervals so a NATS reconnect is noticed
                        # soon and low-rate subjects don't sit on a stale session.
                        blocking_interval = 2.0
                        while not self.reader._reconnect_needed.is_set():
                            self.raise_if_cancelled()
                            log.debug(self.fmt(f"Waiting for messages ({blocking_interval}s)"))
                            try:
                                new_msgs = await session.subscription.fetch(1, timeout=blocking_interval)
                                if new_msgs:
                                    log.debug(self.fmt(f"Received {len(new_msgs)} new message(s)"))
                                    break
                            except asyncio.TimeoutError:
                                # Normal: no message came. It is also all a broken session would
                                # show here, so the main loop asks the server now and then - a
                                # no_wait pull is always answered, with messages or without
                                if time.monotonic() - self.last_probe > self.reader.probe_interval:
                                    break
                            except nats.js.errors.ServiceUnavailableError:
                                session.gone = True  # nobody answers the pull requests: no such consumer
                                break
                            except Exception as e:
                                burst_log.warning("Waiting for messages failed",
                                                  self.fmt(f"Error waiting for messages: {e}"))
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

        def give_up(error: Exception):
            log.error(st.fmt(f"retry budget exhausted "
                             f"(attempts={self._retry.attempts}, "
                             f"total_timeout={self._retry.total_timeout}s), "
                             f"raising: {error}"))
            translated = translate_nats_error(error, f"Reading from '{self.subject}' failed")
            if translated is error:
                raise error
            raise translated from error

        def recovery_deadline() -> float | None:
            """Seconds left for the reader to recover, None when nothing limits this pass

            `total_timeout` is the deadline of a failure: it limits the attempts to recover
            as well as the pauses between them. A healthy reader waits for messages for ever.
            """
            if st.error is None or self.error_behavior != 'WAIT' or self._retry.total_timeout == UNLIMITED:
                return None
            return max(self._retry.total_timeout - (time.monotonic() - st.error_since), 0.0)

        # The loop which tries hard to get some data
        while True:
            st.raise_if_cancelled()
            failing = st.error
            try:
                async with asyncio.timeout(recovery_deadline()) as deadline, st.limited_by(deadline):
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
                    # 3. Replace the consumer session if it stopped serving us
                    await st.ensure_session()
                    # 4. Pull batch of  messages
                    await st.read_batch()
            except TimeoutError:
                if not deadline.expired():
                    raise  # not the deadline of this pass: nothing here may let one out
                if st.error is not None:  # else: recovered in the very moment the deadline fired
                    st.logput('deadline')
                    give_up(failing)  # the failure outlasted total_timeout in the middle of an attempt
            except st.ContinueException as e:  # one of the method did something
                st.logput(f'{e.task}-ok')
                log.debug(st.fmt(f"continue after: {e.task}"))
            except st.ReturnException as e:  # we have data to return
                st.logput(f'{e.task}-ret')
                st.recovered()
                log.debug(st.fmt(f"data returned"))
                return e.data, e.meta
            except st.EndIterationException as e:
                st.logput(f'{e.task}-fin')
                log.info(st.fmt(f"iteration stoped on request"))
                raise MessengerReaderStopped
            except st.ErrorException as e:  # some error
                st.logput(f'{e.task}-err')
                if st.error is None:
                    st.error_since = time.monotonic()  # new error streak begins
                    st.retries = 0
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
                # A missing consumer is not among them: ephemeral consumers come and go,
                # the session is replaced. A missing stream is a deployment error.
                # Neither is a stream which takes no more consumers: a slot may be freed.
                cause = e.error.__cause__ if isinstance(e.error, MessengerJetStreamError) else e.error
                refused = isinstance(cause, nats.js.errors.BadRequestError) and not is_consumer_limit(cause)
                if refused or is_missing_stream(cause):
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
                        elapsed = time.monotonic() - st.error_since
                        if not self._retry.keeps_retrying(st.retries, elapsed):
                            give_up(e.error)
                        wait_time = self._retry.delay_for(st.retries, elapsed)
                        st.retries += 1
                        burst_log.warning("Reading failed, retrying",
                                          st.fmt(f"read_next error, (retry in {wait_time:.1f}s): {e.error}"))
                        await asyncio.sleep(wait_time)
                        if time.monotonic() - st.error_since >= self._retry.total_timeout:
                            give_up(e.error)  # the deadline passed during the pause: no retry after it
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

        session = self._session
        pull_subscription = session.subscription
        msgs = []
        needed = batch
        queue = pull_subscription._sub._pending_queue
        self._server_answered = False

        # First, drain messages already in local queue
        while not queue.empty() and needed > 0:
            try:
                msg = queue.get_nowait()
                pull_subscription._sub._pending_size -= len(msg.data)
                status = JetStreamContext.is_status_msg(msg)
                if not status:  # Skip status messages
                    msgs.append(msg)
                    needed -= 1
                    # delivered by the server to this very queue: the session works
                    self._server_answered = True
                    session.unanswered = 0
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
                burst_log.warning("No server response",
                                  f"{self} Timeout after {timeout}s waiting for server response (network issue?)")

        while True:
            remaining = timeout - (time.monotonic() - start_time)
            if remaining <= 0:
                log_backstop()
                break

            try:
                async with asyncio.timeout(remaining):
                    msg = await queue.get()
            except asyncio.TimeoutError:
                log_backstop()
                break
            except Exception as e:
                log.warning(f"{self} Error in fetch_available: {e}")
                break
            pull_subscription._sub._pending_size -= len(msg.data)
            queue.task_done()
            self._server_answered = True  # a message or a status: the server is there
            session.unanswered = 0

            status = JetStreamContext.is_status_msg(msg)
            if status == "404":
                # Server says: no messages available at all
                log.debug(f"{self} Server confirmed no messages (404 status)")
                break
            elif status == "408":
                # Request Timeout - server closed the request after a partial batch
                log.debug(f"{self} Server closed partial batch (408 status)")
                break
            elif status == "503" or (status == "409" and "Consumer Deleted" in self._status_description(msg)):
                # No responders for the pull request / the consumer deleted under it:
                # the server is there, the consumer is not
                log.debug(f"{self} Server reports the consumer is gone ({status} status)")
                session.gone = True
                break
            elif status == "100":
                continue  # a heartbeat, the answer is still to come
            elif status:
                # The server refused the request (409: a limit of the consumer, a change of the
                # cluster). Not "no data": the reader would never get any and call itself healthy
                description = self._status_description(msg)
                raise MessengerJetStreamError(
                    f"The server refused a pull request for '{self.subject}': {status} {description}",
                    code=int(status), description=description)

            # Real message
            msgs.append(msg)
            needed -= 1

            num_pending = self._num_pending(msg)
            if num_pending is not None:
                self._server_pending = num_pending  # feeds dynamic batch sizing
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

        if not self._server_answered:
            session.unanswered += 1
        return msgs

    @staticmethod
    def _status_description(msg: Msg) -> str:
        """Returns the description of a JetStream status message, '' if it has none"""
        return (msg.headers or {}).get('Description') or ''

    def _next_batch_size(self) -> int:
        """Pull size for the next request.

        Static mode: the configured constant. Dynamic mode (the default):
        sized so that a prefetched message waits at most `max_time_in_memory`
        seconds at the measured consumption rate, damped to at most 8x the
        previous pull (a two-sample rate estimate can be wildly optimistic -
        the damping turns a potential memory spike into a geometric ramp-up),
        clamped to `max_batch` and capped by the server-stamped num_pending
        (+1 to keep probing for new data).
        """
        dynamic = (self.batching.dynamic if self.batching.dynamic is not None
                   else self.batch_defaults.dynamic)
        if not dynamic:
            # the policy wins; otherwise honor the legacy mutable `reader.batch`
            # attribute (initialized to the static default)
            return self.batching.max_batch or self.batch
        max_batch = self.batching.max_batch or self.batch_defaults.max_batch
        rate = self._consumption_rate()
        if rate is None:
            batch = self.batching.initial_batch or self.batch_defaults.initial_batch
        else:
            target = self.batching.max_time_in_memory or self.batch_defaults.max_time_in_memory
            batch = int(rate * target)
            if self._last_batch is not None:
                batch = min(batch, self._last_batch * 8)
        if self._server_pending is not None:
            batch = min(batch, self._server_pending + 1)
        batch = max(1, min(batch, max_batch))
        self._last_batch = batch
        return batch

    def _consumption_rate(self) -> float | None:
        """Messages/s the consumer actually takes, None until enough data"""
        stamps = self._consume_stamps
        if len(stamps) < 2:
            return None
        span = stamps[-1] - stamps[0]
        if span <= 0:
            return None
        return (len(stamps) - 1) / span

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
        if self._session is not None:
            raise MessengerReaderAlreadyOpen("Reader already open, do not reuse MsgReader instances")

        log.debug(f"Opening {self}")
        self.connection.add_reconnect_cb(self.on_nats_reconnect)
        self._session = await self._create_session()
        await super().open()

    async def _create_session(self, resume: bool = False) -> _ConsumerSession:
        """Creates a consumer on the server and the subscription its answers arrive on

        One attempt, all or nothing: on any failure, a cancellation included, nothing
        is left behind for the reader to hold.

        Args:
            resume: continue after the last message read, instead of the reader's delivery policy

        Raises:
            MessengerReaderConfigError: no stream stores the subject, or the server refuses the reader
            MessengerNotConnected: no connection to subscribe through
            MessengerRequestTimeout: the server did not create the consumer in time
            MessengerTransportError: any other failure of the underlying NATS client
        """
        if self.connection is None or self.connection.js is None:
            raise MessengerNotConnected('Cannot create pull subscription, not connected to NATS')
        js = self.connection.js
        consumer_conf = await self._create_consumer_cfg()
        if resume and self.last_seq is not None:
            log.info(f"New consumer session of {self} continues from seq={self.last_seq + 1}")
            consumer_conf.deliver_policy = DeliverPolicy.BY_START_SEQUENCE
            consumer_conf.opt_start_time = None
            consumer_conf.opt_start_seq = self.last_seq + 1
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
                subscription = await js.pull_subscribe(subject,
                                                       durable=consumer_conf.durable_name,
                                                       stream=stream,
                                                       config=consumer_conf)
            except nats.errors.TimeoutError as e:
                raise MessengerRequestTimeout(
                    f"Creating consumer for '{self.subject}' timed out: {e}") from e
            except nats.js.errors.NotFoundError as e:
                if is_missing_stream(e):
                    raise MessengerReaderConfigError(
                        f"No stream stores the subject '{self.subject}': {e}") from e
                raise
            try:
                ci = await subscription.consumer_info()
                if filters:
                    await self._verify_family_filters(subscription, ci, filters)
            except BaseException:
                # Subscribed, but not a session yet: nobody would ever hold this subscription.
                # The consumer, if it was created, expires by itself - nobody pulls from it.
                try:
                    await subscription.unsubscribe()
                except Exception as e:
                    log.debug(f"{self} could not drop the subscription of a failed session: {e}")
                raise
        return _ConsumerSession(subscription, stream=ci.stream_name, consumer=ci.name)

    async def _consumer_is_gone(self, session: _ConsumerSession) -> bool:
        """Tells whether the server knows the consumer of the session no more

        Asked only when a pull request got no answer, never as a routine: a consumer
        which exists says nothing about the session being usable.

        Raises:
            nats.js.errors.NotFoundError: the stream itself is gone
        """
        try:
            await session.subscription.consumer_info()
        except nats.js.errors.NotFoundError as e:
            if is_missing_stream(e):
                raise
            return True
        except Exception as e:
            log.debug(f"{self} no answer about the consumer either: {e}")
        return False

    async def _replace_session(self) -> None:
        """Gives the reader a new consumer session, continuing after the last message read

        The new session is complete before the old one is touched, so a failure leaves
        the reader exactly as it was: with a session it can use, or try to replace again.
        One attempt - whoever calls paces the retries (the reader loop: its RetryPolicy).

        Two cases cannot have "the new one first", and give the old consumer up before:

        - a durable consumer is its name. It is created anew, right after the last message
          read; binding the existing one would leave what it has delivered and the reader
          has not read to be redelivered later, out of order;
        - a stream which takes no more consumers has no slot for a second one.
        """
        old = self._session
        if old is not None and self.consumer_cfg.get('durable_name'):
            await self._delete_consumer(old)
            old.gone = True  # should the creation fail: this session has nothing to read from
        try:
            new = await self._create_session(resume=True)
        except MessengerJetStreamError as e:
            if old is None or old.gone or not is_consumer_limit(e.__cause__):
                raise
            await self._discard_session(old)
            old, self._session = None, None
            new = await self._create_session(resume=True)
        self._session = new
        self._reconnect_count += 1
        if old is not None:
            await self._discard_session(old, keep_consumer=old.gone)

    async def _identify(self, session: _ConsumerSession) -> tuple[str, str]:
        """Returns the stream and the name of the session's consumer, asking the server if not known"""
        if session.consumer is None:  # a subscription assigned from outside
            ci = await session.subscription.consumer_info()
            session.stream, session.consumer = ci.stream_name, ci.name
        return session.stream, session.consumer

    async def _delete_consumer(self, session: _ConsumerSession) -> None:
        """Deletes the consumer of the session; one which is not there any more is as good as deleted

        Raises:
            MessengerTransportError: the consumer could not be deleted
        """
        with nats_errors_translated(f"Deleting the consumer of '{self.subject}' failed"):
            try:
                stream, consumer = await self._identify(session)
                await self.connection.js.delete_consumer(stream=stream, consumer=consumer)
            except nats.js.errors.NotFoundError:
                pass

    async def _discard_session(self, session: _ConsumerSession, keep_consumer: bool = False) -> None:
        """Removes a session the reader does not hold any more; best effort, never fails

        Whatever cannot be removed now removes itself: a consumer nobody pulls from
        expires on the server (`inactive_threshold`).

        Args:
            keep_consumer: drop the subscription only - there is no consumer of this session to delete
        """
        try:
            await session.subscription.unsubscribe()
        except Exception as e:
            log.debug(f"{self} could not unsubscribe a discarded session: {e}")
        if keep_consumer:
            return
        try:
            await self._delete_consumer(session)
        except Exception as e:
            log.debug(f"{self} could not delete the consumer of a discarded session (it will expire): {e}")

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
            MessengerReaderConfigError: the family is split between storages (the bare subject
                apart from the keyed ones, or the keyed ones among themselves) - no single
                reader can deliver it whole, and a part of it would be a silent lie
        """
        storages: dict[str, list[str]] = {}  # storage -> family patterns it holds messages of
        for pattern in subject_family(self.subject):
            for storage in await self.connection.subject_storages(pattern):
                storages.setdefault(storage, []).append(pattern)
        if len(storages) > 1:
            places = ', '.join(f"{' and '.join(patterns)} in '{storage}'" for storage, patterns in storages.items())
            raise MessengerReaderConfigError(
                f"The subject family of '{self.subject}' is stored in several places ({places}) and cannot "
                f"be read as one. Store the family together, or read its parts with separate readers")
        for storage, patterns in storages.items():
            return storage, patterns
        return None, []

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
        session, self._session = self._session, None
        if session is not None:
            await self._discard_session(session)
        await self._close_push_subscription()

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
                async with asyncio.timeout(to):
                    await self._msg_processed.wait()
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
                - reconnect_count: Number of times the consumer session was replaced
                - last_error: String representation of last error or None
                - last_health_check_time: Timestamp (monotonic time) of the last pull request the server answered, or None
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
                   deliver_policy: str | DeliveryPolicy = 'all',
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
