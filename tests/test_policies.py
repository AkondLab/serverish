"""Unit tests for policy objects (docs/design/POLICIES.md) - no NATS needed."""
import dataclasses
import datetime

import pytest

from serverish.messenger import (UNLIMITED, BatchPolicy, DeliverAll, DeliverFromSeq,
                                 DeliverFromTime, DeliverLast, DeliverLastPerSubject,
                                 DeliverNew, DeliveryPolicy, ErrorPolicy, OnError,
                                 OnMissed, PolicyConflict, RetryPolicy, Until)

UTC = datetime.timezone.utc


# --- DeliveryPolicy -------------------------------------------------------

def test_from_value_string_sugar():
    assert DeliveryPolicy.from_value('all') == DeliverAll()
    assert DeliveryPolicy.from_value('last') == DeliverLast()
    assert DeliveryPolicy.from_value('new') == DeliverNew()
    assert DeliveryPolicy.from_value('last_per_subject') == DeliverLastPerSubject()


def test_from_value_with_satellites():
    t = datetime.datetime.now(UTC)
    p = DeliveryPolicy.from_value('by_start_time', opt_start_time=t, nowait=True)
    assert p == DeliverFromTime(t, until=Until.END_OF_DATA)
    p = DeliveryPolicy.from_value('by_start_sequence', opt_start_seq=42)
    assert p == DeliverFromSeq(42)


def test_from_value_passes_objects_through():
    p = DeliverLast(until=Until.END_OF_DATA)
    assert DeliveryPolicy.from_value(p) is p


def test_from_value_object_plus_satellite_conflicts():
    with pytest.raises(PolicyConflict):
        DeliveryPolicy.from_value(DeliverLast(), nowait=True)
    with pytest.raises(PolicyConflict):
        DeliveryPolicy.from_value(DeliverAll(), opt_start_time=datetime.datetime.now(UTC))


def test_from_value_missing_start_markers():
    with pytest.raises(ValueError, match="requires opt_start_time"):
        DeliveryPolicy.from_value('by_start_time')
    with pytest.raises(ValueError, match="requires opt_start_seq"):
        DeliveryPolicy.from_value('by_start_sequence')
    with pytest.raises(ValueError, match="Unknown deliver policy"):
        DeliveryPolicy.from_value('sometimes')


def test_variant_validation():
    with pytest.raises(ValueError):
        DeliverFromTime(None)
    with pytest.raises(ValueError):
        DeliverFromSeq(0)
    with pytest.raises(ValueError):
        DeliverFromSeq(True)  # bool is not a sequence number


def test_to_reader_params():
    t = datetime.datetime.now(UTC)
    assert DeliverLast().to_reader_params() == {'deliver_policy': 'last'}
    assert DeliverFromTime(t, until=Until.END_OF_DATA).to_reader_params() == {
        'deliver_policy': 'by_start_time', 'opt_start_time': t, 'nowait': True}
    assert DeliverFromSeq(7).to_reader_params() == {
        'deliver_policy': 'by_start_sequence', 'opt_start_seq': 7}


def test_policies_are_frozen():
    with pytest.raises(dataclasses.FrozenInstanceError):
        DeliverLast().until = Until.FOREVER
    with pytest.raises(dataclasses.FrozenInstanceError):
        RetryPolicy().attempts = 5
    with pytest.raises(dataclasses.FrozenInstanceError):
        BatchPolicy().max_batch = 10


# --- RetryPolicy ----------------------------------------------------------

def test_retry_validation():
    with pytest.raises(ValueError):
        RetryPolicy(attempts=-1)
    with pytest.raises(ValueError):
        RetryPolicy(attempts=1.5)
    with pytest.raises(ValueError):
        RetryPolicy(backoff=0.5)
    with pytest.raises(ValueError):
        RetryPolicy(delay=-1)
    RetryPolicy(attempts=UNLIMITED, total_timeout=UNLIMITED)  # explicit no-limit is fine


def test_retry_resolved_fills_only_none():
    defaults = RetryPolicy(attempts=2, delay=1.0, backoff=2.0, max_delay=8.0, total_timeout=UNLIMITED)
    r = RetryPolicy(attempts=5).resolved(defaults)
    assert r == RetryPolicy(attempts=5, delay=1.0, backoff=2.0, max_delay=8.0, total_timeout=UNLIMITED)


def test_retry_delay_for_backoff_and_cap():
    r = RetryPolicy(attempts=UNLIMITED, delay=1.0, backoff=2.0, max_delay=5.0, total_timeout=UNLIMITED)
    assert [r.delay_for(n) for n in range(4)] == [1.0, 2.0, 4.0, 5.0]


def test_retry_keeps_retrying():
    r = RetryPolicy(attempts=2, delay=0, backoff=1, max_delay=0, total_timeout=10.0)
    assert r.keeps_retrying(0, elapsed=0)
    assert r.keeps_retrying(1, elapsed=9.9)
    assert not r.keeps_retrying(2, elapsed=0)          # attempts exhausted
    assert not r.keeps_retrying(0, elapsed=11.0)       # time budget exhausted
    unlimited = RetryPolicy(attempts=UNLIMITED, total_timeout=UNLIMITED)
    assert unlimited.keeps_retrying(10 ** 6, elapsed=10.0 ** 6)


# --- ErrorPolicy ----------------------------------------------------------

def test_error_policy_from_legacy_kwargs():
    p = ErrorPolicy.from_kwargs(None, error_behavior='FINISH', on_missed_messages='REPLAY')
    assert p == ErrorPolicy(on_error=OnError.FINISH, on_missed=OnMissed.REPLAY)
    p = ErrorPolicy.from_kwargs(None, raise_on_publish_error=False, ack_timeout_retries=5)
    assert p.on_error is OnError.LOG
    assert p.retry.attempts == 5
    assert ErrorPolicy.from_kwargs(None) is None  # nothing given -> pure driver defaults


def test_error_policy_conflicts():
    with pytest.raises(PolicyConflict):
        ErrorPolicy.from_kwargs(ErrorPolicy(on_error=OnError.RAISE), error_behavior='WAIT')
    with pytest.raises(PolicyConflict):
        ErrorPolicy.from_kwargs(ErrorPolicy(), ack_timeout_retries=1)


# --- BatchPolicy ----------------------------------------------------------

def test_batch_validation():
    with pytest.raises(ValueError):
        BatchPolicy(max_batch=0)
    with pytest.raises(ValueError):
        BatchPolicy(dynamic=False, max_time_in_memory=1.0)
    with pytest.raises(ValueError):
        BatchPolicy(dynamic=False, initial_batch=10)
    with pytest.raises(ValueError):
        BatchPolicy(dynamic=True, initial_batch=200, max_batch=100)
    BatchPolicy(max_batch=500)                                # mode-neutral: just the bound
    BatchPolicy(dynamic=True, max_batch=1000, max_time_in_memory=0.5)


# --- driver integration (construction only, no NATS) -----------------------

def test_reader_accepts_delivery_object():
    from serverish.messenger import get_reader
    t = datetime.datetime.now(UTC)
    reader = get_reader('test.policies.unit', deliver_policy=DeliverFromTime(t, until=Until.END_OF_DATA))
    assert reader.deliver_policy == 'by_start_time'
    assert reader.opt_start_time == t
    assert reader.nowait is True
    assert reader.delivery == DeliverFromTime(t, until=Until.END_OF_DATA)


def test_reader_legacy_kwargs_still_normalize():
    from serverish.messenger import get_reader
    reader = get_reader('test.policies.unit', deliver_policy='last', nowait=True)
    assert reader.delivery == DeliverLast(until=Until.END_OF_DATA)
    assert reader.nowait is True


def test_reader_policy_satellite_conflict():
    from serverish.messenger import get_reader
    with pytest.raises(PolicyConflict):
        get_reader('test.policies.unit', deliver_policy=DeliverLast(), nowait=True)
    with pytest.raises(PolicyConflict):
        get_reader('test.policies.unit', deliver_policy=DeliverAll(),
                   error_policy=ErrorPolicy(on_error=OnError.WAIT), error_behavior='RAISE')


def test_reader_rejects_log_on_error():
    from serverish.messenger import get_reader
    with pytest.raises(ValueError, match="OnError.LOG"):
        get_reader('test.policies.unit', error_policy=ErrorPolicy(on_error=OnError.LOG))


def test_reader_retry_resolution():
    from serverish.messenger import get_reader
    reader = get_reader('test.policies.unit',
                        error_policy=ErrorPolicy(retry=RetryPolicy(attempts=3, delay=0.5)))
    assert reader._retry.attempts == 3
    assert reader._retry.delay == 0.5
    assert reader._retry.max_delay == 15.0  # filled from driver defaults


def test_reader_batch_defaults_are_dynamic():
    """Driver default: dynamic sizing, initial 2, max 10k (docs/design/POLICIES.md)."""
    from serverish.messenger import get_reader
    reader = get_reader('test.policies.unit')
    assert reader._next_batch_size() == 2          # initial pull is tiny
    # a mode-neutral policy (just the bound) inherits the dynamic default
    bounded = get_reader('test.policies.unit', batch_policy=BatchPolicy(max_batch=500))
    assert bounded._next_batch_size() == 2


def test_reader_static_batch_from_policy():
    from serverish.messenger import get_reader
    reader = get_reader('test.policies.unit', batch_policy=BatchPolicy(dynamic=False, max_batch=7))
    assert reader._next_batch_size() == 7
    plain_static = get_reader('test.policies.unit', batch_policy=BatchPolicy(dynamic=False))
    assert plain_static._next_batch_size() == 100  # static default size


def test_reader_dynamic_batch_sizing():
    from serverish.messenger import get_reader
    reader = get_reader('test.policies.unit',
                        batch_policy=BatchPolicy(dynamic=True, max_batch=1000,
                                                 initial_batch=50, max_time_in_memory=2.0))
    # no consumption data yet -> initial batch
    assert reader._next_batch_size() == 50
    # consumer measured at ~100 msg/s -> 100 * 2.0s = 200 (within 8x damping of 50)
    reader._consume_stamps.extend(i * 0.01 for i in range(11))
    assert reader._next_batch_size() == 200
    # server says only 20 pending -> capped at 21 (probe for new data)
    reader._server_pending = 20
    assert reader._next_batch_size() == 21
    # max_batch clamp wins over huge rate (damping released for the check)
    reader._server_pending = None
    reader._last_batch = None
    reader._consume_stamps.clear()
    reader._consume_stamps.extend(i * 0.0001 for i in range(11))
    assert reader._next_batch_size() == 1000


def test_reader_dynamic_growth_damping():
    """A burst-inflated rate estimate must ramp geometrically, not spike."""
    from serverish.messenger import get_reader
    reader = get_reader('test.policies.unit')  # defaults: initial 2, max 10k
    assert reader._next_batch_size() == 2
    # absurdly fast consumption measured -> still at most 8x per step
    reader._consume_stamps.extend(i * 0.0001 for i in range(11))
    assert reader._next_batch_size() == 16
    assert reader._next_batch_size() == 128
    assert reader._next_batch_size() == 1024


def test_driver_error_defaults():
    """Per-driver defaults documented at the driver (POLICIES.md §1.4)."""
    from serverish.messenger import get_publisher, get_journalpublisher
    from serverish.messenger.msg_reader import MsgReader
    from serverish.messenger.msg_publisher import MsgPublisher
    assert get_publisher('test.policies.unit').raise_on_publish_error is True
    assert get_journalpublisher('test.policies.unit').raise_on_publish_error is False
    assert MsgPublisher.retry_defaults.attempts == 2
    assert MsgReader.retry_defaults.attempts == UNLIMITED
    assert MsgReader.retry_defaults.max_delay == 15.0


async def test_driver_close_deregisters_from_tree():
    """Closed drivers must leave the Messenger tree (no unbounded accumulation),
    and reopening must re-register them."""
    from serverish.messenger import Messenger, get_publisher
    m = Messenger()
    pub = get_publisher('test.policies.unit.lifecycle')
    assert pub in m.children_names
    await pub.close()
    assert pub not in m.children_names
    assert pub.parent is m  # kept for reopen
    await pub.open()
    assert pub in m.children_names
    await pub.close()
    assert pub not in m.children_names


def test_publisher_error_policy():
    from serverish.messenger import get_publisher
    pub = get_publisher('test.policies.unit',
                        error_policy=ErrorPolicy(on_error=OnError.LOG,
                                                 retry=RetryPolicy(attempts=0)))
    assert pub.raise_on_publish_error is False
    assert pub._ack_retry.attempts == 0
    with pytest.raises(ValueError, match="RAISE or"):
        get_publisher('test.policies.unit', error_policy=ErrorPolicy(on_error=OnError.WAIT))
    with pytest.raises(PolicyConflict):
        get_publisher('test.policies.unit', error_policy=ErrorPolicy(on_error=OnError.RAISE),
                      raise_on_publish_error=False)


def test_publisher_legacy_ack_retries_stay_live():
    from serverish.messenger import get_publisher
    pub = get_publisher('test.policies.unit')
    pub.ack_timeout_retries = 7  # legacy post-construction mutation
    assert pub._ack_retry.attempts == 7


# --- deprecations ----------------------------------------------------------

def test_single_factories_deprecated():
    from serverish.messenger import get_singlepublisher, get_singlereader
    with pytest.warns(DeprecationWarning, match="single_publish"):
        get_singlepublisher('test.policies.unit')
    with pytest.warns(DeprecationWarning, match="single_read"):
        get_singlereader('test.policies.unit')
