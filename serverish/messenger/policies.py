"""Policy objects for Messenger drivers.

Policies group related driver configuration into typed, immutable value
objects (design: docs/design/POLICIES.md):

- `DeliveryPolicy` variants — where a reader starts and when it stops,
- `ErrorPolicy` (+ nested `RetryPolicy`) — what to do when things break,
- `BatchPolicy` — how much a reader prefetches.

All policies are frozen dataclasses: immutable, hashable, safe to share as
class-level defaults. Two uniform rules apply everywhere:

- ``None`` always and only means "driver default" — each driver documents
  its effective defaults;
- "no limit" is always the explicit `UNLIMITED`, never a hidden meaning
  of None.

Backward compatibility: the flat kwargs used across the ecosystem
(``deliver_policy='last'``, ``opt_start_time=...``, ``nowait=True``,
``error_behavior=...``, ``raise_on_publish_error=...``) keep working —
they are normalized into policies in one place (`resolve_delivery`,
`resolve_error_policy`). Passing a policy object together with one of its
satellite kwargs is an error.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import Enum

UNLIMITED = math.inf
"""Explicit "no limit" marker for numeric policy fields.

Deliberately NOT named FOREVER: `Until.FOREVER` is a different concept
("keep following live data").
"""


class Until(Enum):
    """When a reader finishes iterating."""
    FOREVER = 'forever'          # live subscription, never finishes (legacy: nowait=False)
    END_OF_DATA = 'end_of_data'  # stop when the server confirms drained (legacy: nowait=True)


class OnError(Enum):
    """Reaction to a serious driver error (e.g. disconnection, no ack)."""
    RAISE = 'RAISE'    # re-raise to the caller
    FINISH = 'FINISH'  # silently finish iteration (readers)
    WAIT = 'WAIT'      # wait for recovery, keep retrying (readers)
    LOG = 'LOG'        # log and continue, tag the message (publishers)


class OnMissed(Enum):
    """Reaction to messages missed during a broken connection (readers)."""
    SKIP = 'SKIP'
    REPLAY = 'REPLAY'


class PolicyConflict(ValueError):
    """Raised when a policy object is combined with one of its satellite kwargs."""
    pass


# --------------------------------------------------------------------------
# DeliveryPolicy — variants as types: invalid combinations are unrepresentable
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class DeliveryPolicy:
    """Base of delivery policy variants - where to start, when to stop.

    Use one of the concrete variants: `DeliverAll`, `DeliverLast`,
    `DeliverNew`, `DeliverLastPerSubject`, `DeliverFromTime(start)`,
    `DeliverFromSeq(seq)`.
    """
    until: Until | None = field(default=None, kw_only=True)

    _policy_str: str = field(default='', init=False, repr=False, compare=False)

    @classmethod
    def from_value(cls, value: 'str | DeliveryPolicy',
                   opt_start_time: datetime | None = None,
                   opt_start_seq: int | None = None,
                   nowait: bool | None = None) -> 'DeliveryPolicy':
        """Normalizes legacy flat kwargs or a policy object into a policy object.

        The string form is permanent API sugar (``'last'`` == `DeliverLast()`),
        not deprecated. Satellite kwargs (`opt_start_time`, `opt_start_seq`,
        `nowait`) are accepted only with the string form.

        Raises:
            PolicyConflict: a policy object was combined with a satellite kwarg
            ValueError: unknown policy string or missing start marker
        """
        if isinstance(value, DeliveryPolicy):
            for name, sat in (('opt_start_time', opt_start_time),
                              ('opt_start_seq', opt_start_seq),
                              ('nowait', nowait)):
                if sat is not None:
                    raise PolicyConflict(
                        f"Both a DeliveryPolicy object and legacy '{name}' given - "
                        f"express everything in the policy object")
            return value
        until = None if nowait is None else (Until.END_OF_DATA if nowait else Until.FOREVER)
        match value:
            case 'all':
                policy = DeliverAll()
            case 'last':
                policy = DeliverLast()
            case 'new':
                policy = DeliverNew()
            case 'last_per_subject':
                policy = DeliverLastPerSubject()
            case 'by_start_time':
                if opt_start_time is None:
                    raise ValueError("deliver_policy='by_start_time' requires opt_start_time to be set")
                policy = DeliverFromTime(opt_start_time)
            case 'by_start_sequence':
                if opt_start_seq is None:
                    raise ValueError("deliver_policy='by_start_sequence' requires opt_start_seq "
                                     "to be set in consumer_cfg")
                policy = DeliverFromSeq(opt_start_seq)
            case _:
                raise ValueError(f"Unknown deliver policy {value!r}")
        if until is not None:
            policy = replace(policy, until=until)
        return policy

    def to_reader_params(self) -> dict:
        """Translates the policy into MsgReader's canonical parameters.

        This (together with `MsgReader._create_consumer_cfg`) is the single
        point where serverish delivery semantics map onto the NATS API.
        """
        params: dict = {'deliver_policy': self._policy_str}
        if self.until is not None:
            params['nowait'] = self.until is Until.END_OF_DATA
        return params


@dataclass(frozen=True)
class DeliverAll(DeliveryPolicy):
    """Deliver all messages retained in the stream."""
    _policy_str: str = field(default='all', init=False, repr=False, compare=False)


@dataclass(frozen=True)
class DeliverLast(DeliveryPolicy):
    """Deliver the last message of the subject, then live data."""
    _policy_str: str = field(default='last', init=False, repr=False, compare=False)


@dataclass(frozen=True)
class DeliverNew(DeliveryPolicy):
    """Deliver only messages published after subscribing."""
    _policy_str: str = field(default='new', init=False, repr=False, compare=False)


@dataclass(frozen=True)
class DeliverLastPerSubject(DeliveryPolicy):
    """Deliver the last message of each subject matching a wildcard (snapshot)."""
    _policy_str: str = field(default='last_per_subject', init=False, repr=False, compare=False)


@dataclass(frozen=True)
class DeliverFromTime(DeliveryPolicy):
    """Deliver messages published at or after `start` (tz-aware datetime,
    or - legacy - an ISO string passed through to the server)."""
    start: datetime | str = None  # type: ignore[assignment]  # required, validated below
    _policy_str: str = field(default='by_start_time', init=False, repr=False, compare=False)

    def __post_init__(self):
        if not isinstance(self.start, (datetime, str)):
            raise ValueError("DeliverFromTime requires a datetime (or ISO string) 'start'")

    def to_reader_params(self) -> dict:
        params = super().to_reader_params()
        params['opt_start_time'] = self.start
        return params


@dataclass(frozen=True)
class DeliverFromSeq(DeliveryPolicy):
    """Deliver messages starting from stream sequence `seq`."""
    seq: int = None  # type: ignore[assignment]  # required, validated below
    _policy_str: str = field(default='by_start_sequence', init=False, repr=False, compare=False)

    def __post_init__(self):
        if not isinstance(self.seq, int) or isinstance(self.seq, bool) or self.seq < 1:
            raise ValueError("DeliverFromSeq requires a positive int 'seq'")

    def to_reader_params(self) -> dict:
        params = super().to_reader_params()
        params['opt_start_seq'] = self.seq
        return params


# --------------------------------------------------------------------------
# ErrorPolicy / RetryPolicy
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class RetryPolicy:
    """How to retry a failing operation.

    All fields default to None = "driver default". `UNLIMITED` is the
    explicit no-limit marker for `attempts` and `total_timeout`.
    """
    attempts: int | float | None = None      # 0 = no retry; UNLIMITED = keep retrying
    delay: float | None = None               # initial inter-attempt delay [s]
    backoff: float | None = None             # delay multiplier per attempt (>= 1)
    max_delay: float | None = None           # cap for the growing delay [s]
    total_timeout: float | None = None       # overall time budget [s]; UNLIMITED = none

    def __post_init__(self):
        if self.attempts is not None and self.attempts != UNLIMITED and (
                not isinstance(self.attempts, int) or self.attempts < 0):
            raise ValueError("RetryPolicy.attempts must be a non-negative int or UNLIMITED")
        if self.backoff is not None and self.backoff < 1:
            raise ValueError("RetryPolicy.backoff must be >= 1")
        for name in ('delay', 'max_delay', 'total_timeout'):
            v = getattr(self, name)
            if v is not None and v != UNLIMITED and v < 0:
                raise ValueError(f"RetryPolicy.{name} must be non-negative")

    def resolved(self, defaults: 'RetryPolicy') -> 'RetryPolicy':
        """Returns a copy with None fields filled from `defaults` (driver defaults)."""
        return RetryPolicy(*(own if own is not None else default
                             for own, default in zip(
                                 (self.attempts, self.delay, self.backoff,
                                  self.max_delay, self.total_timeout),
                                 (defaults.attempts, defaults.delay, defaults.backoff,
                                  defaults.max_delay, defaults.total_timeout))))

    def delay_for(self, attempt: int) -> float:
        """Delay before retry number `attempt` (0-based), on a fully resolved policy."""
        delay = (self.delay or 0.0) * (self.backoff or 1.0) ** attempt
        max_delay = self.max_delay
        if max_delay is not None and max_delay != UNLIMITED:
            delay = min(delay, max_delay)
        return delay

    def keeps_retrying(self, attempt: int, elapsed: float) -> bool:
        """Whether retry number `attempt` (0-based) is allowed, on a fully resolved policy."""
        attempts = self.attempts if self.attempts is not None else 0
        if attempts != UNLIMITED and attempt >= attempts:
            return False
        total = self.total_timeout
        if total is not None and total != UNLIMITED and elapsed >= total:
            return False
        return True


@dataclass(frozen=True)
class ErrorPolicy:
    """What to do when things break - common to readers and publishers.

    Readers use `on_error` RAISE/FINISH/WAIT and `on_missed`; publishers use
    `on_error` RAISE/LOG. `retry` paces WAIT-mode reconnects (readers) and
    ack-timeout retries (publishers). None = driver default.
    """
    on_error: OnError | None = None
    on_missed: OnMissed | None = None
    retry: RetryPolicy | None = None

    @classmethod
    def from_kwargs(cls, error_policy: 'ErrorPolicy | None',
                    error_behavior: str | None = None,
                    on_missed_messages: str | None = None,
                    raise_on_publish_error: bool | None = None,
                    ack_timeout_retries: int | None = None) -> 'ErrorPolicy | None':
        """Normalizes legacy flat kwargs or a policy object into a policy object.

        Returns None when nothing at all was specified (pure driver defaults).

        Raises:
            PolicyConflict: policy object combined with a satellite kwarg
        """
        satellites = {'error_behavior': error_behavior,
                      'on_missed_messages': on_missed_messages,
                      'raise_on_publish_error': raise_on_publish_error,
                      'ack_timeout_retries': ack_timeout_retries}
        given = {k: v for k, v in satellites.items() if v is not None}
        if error_policy is not None:
            if not isinstance(error_policy, ErrorPolicy):
                raise ValueError(f"error_policy must be an ErrorPolicy, got {type(error_policy).__name__}")
            if given:
                raise PolicyConflict(
                    f"Both error_policy and legacy {sorted(given)} given - "
                    f"express everything in the policy object")
            return error_policy
        if not given:
            return None
        on_error = None
        if error_behavior is not None:
            on_error = OnError(error_behavior)
        elif raise_on_publish_error is not None:
            on_error = OnError.RAISE if raise_on_publish_error else OnError.LOG
        on_missed = OnMissed(on_missed_messages) if on_missed_messages is not None else None
        retry = RetryPolicy(attempts=ack_timeout_retries) if ack_timeout_retries is not None else None
        return cls(on_error=on_error, on_missed=on_missed, retry=retry)


# --------------------------------------------------------------------------
# BatchPolicy
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class BatchPolicy:
    """How much a reader prefetches per pull - and how that adapts.

    `max_batch` means the same thing in both modes (static: the pull size;
    dynamic: its upper bound), so callers expressing only the bound stay
    neutral on mode and benefit when the driver default flips to dynamic.

    Dynamic mode (`dynamic=True`): pull size follows the consumer's measured
    consumption rate so a prefetched message waits at most
    `max_time_in_memory` seconds client-side, clamped to `max_batch` and
    capped by the server-stamped num_pending.

    None = driver default (today: static, max_batch=100).
    """
    max_batch: int | None = None
    dynamic: bool | None = None
    initial_batch: int | None = None            # dynamic only: size of the first pull
    max_time_in_memory: float | None = None     # dynamic only: target residency [s]

    def __post_init__(self):
        if self.max_batch is not None and self.max_batch < 1:
            raise ValueError("BatchPolicy.max_batch must be >= 1")
        if self.initial_batch is not None and self.initial_batch < 1:
            raise ValueError("BatchPolicy.initial_batch must be >= 1")
        if self.max_time_in_memory is not None and self.max_time_in_memory <= 0:
            raise ValueError("BatchPolicy.max_time_in_memory must be positive")
        if self.dynamic is False and (self.initial_batch is not None
                                      or self.max_time_in_memory is not None):
            raise ValueError("BatchPolicy: initial_batch/max_time_in_memory require dynamic mode, "
                             "but dynamic=False was given")
        if (self.initial_batch is not None and self.max_batch is not None
                and self.initial_batch > self.max_batch):
            raise ValueError("BatchPolicy.initial_batch must not exceed max_batch")
