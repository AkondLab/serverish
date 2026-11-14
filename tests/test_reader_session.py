"""The consumer session of a reader: whatever fails around it, an open reader reads again by itself.

The cases come from the observatory outage of 2026-11-14, where readers of a long-running
service stayed open and deaf for hours after the network was back.
"""
import asyncio
import json
import uuid

import nats.errors
import nats.js.errors
import pytest

from serverish.base import MessengerJetStreamError, MessengerReaderConfigError
from serverish.connection.nats_errors import is_consumer_limit, is_missing_stream
from serverish.messenger import BatchPolicy, ErrorPolicy, OnError, RetryPolicy, get_publisher, get_reader

pytestmark = [pytest.mark.nats, pytest.mark.timeout(40)]

FAST = ErrorPolicy(on_error=OnError.WAIT, retry=RetryPolicy(delay=0.05, max_delay=0.2))


class ApiDown:
    """JetStream refuses to create consumers, as on a link which lets nothing through in time"""

    def __init__(self, reader, delay: float = 0.0):
        self.js = reader.connection.js
        self.original = self.js.pull_subscribe
        self.delay = delay
        self.attempts = 0

    def __enter__(self):
        async def times_out(*args, **kwargs):
            self.attempts += 1
            await asyncio.sleep(self.delay)
            raise nats.errors.TimeoutError

        self.js.pull_subscribe = times_out
        return self

    def __exit__(self, *exc):
        self.js.pull_subscribe = self.original


class Reading:
    """A reader being read in the background, a publisher to feed it"""

    def __init__(self, subject, **reader_kwargs):
        self.reader = get_reader(subject, deliver_policy='new', **reader_kwargs)
        self.reader.probe_interval = 0.5
        self.publisher = get_publisher(subject)
        self.got: list[int] = []
        self.task: asyncio.Task | None = None

    async def __aenter__(self):
        await self.publisher.open()
        await self.reader.open()
        self.task = asyncio.create_task(self._read())
        await self.publish_and_expect(0)   # the reader works before anything is broken
        return self

    async def __aexit__(self, *exc):
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
        await self.reader.close()
        await self.publisher.close()

    async def _read(self):
        async for data, meta in self.reader:
            self.got.append(data['n'])

    async def publish_and_expect(self, n: int, within: float = 15.0):
        await self.publisher.publish(data={'n': n})
        async with asyncio.timeout(within):
            while n not in self.got:
                assert not self.task.done(), f'the reader ended: {self.task}'
                await asyncio.sleep(0.02)


def test_only_a_missing_stream_is_a_deployment_error():
    assert is_missing_stream(nats.js.errors.NotFoundError())                            # no stream stores the subject
    assert is_missing_stream(nats.js.errors.NotFoundError(code=404, err_code=10059))    # stream not found
    assert not is_missing_stream(nats.js.errors.NotFoundError(code=404, err_code=10014))  # consumer not found
    assert not is_missing_stream(nats.js.errors.ServiceUnavailableError(code=503))
    assert not is_missing_stream(nats.errors.TimeoutError())


def test_a_full_stream_is_not_a_wrong_configuration():
    assert is_consumer_limit(nats.js.errors.BadRequestError(code=400, err_code=10026))    # maximum consumers limit reached
    assert not is_consumer_limit(nats.js.errors.BadRequestError(code=400, err_code=10012))
    assert not is_consumer_limit(nats.js.errors.NotFoundError(code=404, err_code=10014))


async def test_subject_without_a_stream_is_refused_at_open(messenger):
    reader = get_reader('nostream.for.this.subject', deliver_policy='new')
    with pytest.raises(MessengerReaderConfigError, match='No stream stores'):
        await reader.open()
    assert reader.pull_subscription is None


async def test_reader_reads_again_after_its_subscription_is_lost(messenger, unique_subject):
    """The state of the outage itself: the consumer lives, its answers reach nobody."""
    async with Reading(unique_subject, error_policy=FAST) as r:
        lost = r.reader._session
        await lost.subscription.unsubscribe()
        await r.publish_and_expect(1)
        assert r.reader._session is not lost
        assert r.reader.health_status['reconnect_count'] == 1


async def test_failed_replacement_leaves_a_working_reader_working(messenger, unique_subject):
    """A replacement which cannot be made costs nothing: the session in use stays in use."""
    async with Reading(unique_subject, error_policy=FAST) as r:
        session = r.reader._session
        with ApiDown(r.reader) as api:
            r.reader.reconnect()            # what a NATS reconnect asks for
            await r.publish_and_expect(1)   # read while no consumer can be created
            assert api.attempts == 1        # the failed attempt is not repeated: nothing is wrong
        assert r.reader._session is session
        await r.publish_and_expect(2)


async def test_lost_subscription_and_no_way_to_replace_it_for_a_while(messenger, unique_subject):
    """Lost answers while consumers cannot be created: the reader keeps trying, and reads when it can."""
    async with Reading(unique_subject, error_policy=FAST) as r:
        lost = r.reader._session
        with ApiDown(r.reader, delay=0.2) as api:
            await lost.subscription.unsubscribe()
            async with asyncio.timeout(20):
                while api.attempts < 4:
                    await asyncio.sleep(0.05)
            assert r.reader._session is lost    # nothing better was made, nothing was lost on the way
            assert not r.task.done()
        await r.publish_and_expect(1)
        assert r.reader._session is not lost


async def test_expired_consumer_is_replaced_without_an_error(messenger, unique_subject):
    """A consumer gone from the server is ordinary life, also for a reader told to raise on errors."""
    async with Reading(unique_subject, error_policy=ErrorPolicy(on_error=OnError.RAISE)) as r:
        gone = r.reader._session
        await r.reader.connection.js.delete_consumer(stream=gone.stream, consumer=gone.consumer)
        await r.publish_and_expect(1)
        assert r.reader._session is not gone
        assert r.reader._last_error is None


async def test_missing_consumer_reported_by_the_api_does_not_stop_the_reader(messenger, unique_subject):
    async with Reading(unique_subject, error_policy=FAST) as r:
        real = r.reader.fetch_available
        calls = []

        async def consumer_not_found_once(*args, **kwargs):
            calls.append(1)
            if len(calls) == 1:
                raise nats.js.errors.NotFoundError(code=404, err_code=10014, description='consumer not found')
            return await real(*args, **kwargs)

        r.reader.fetch_available = consumer_not_found_once
        try:
            async with asyncio.timeout(10):
                while not calls:
                    await asyncio.sleep(0.05)
            await r.publish_and_expect(1)
        finally:
            del r.reader.fetch_available


async def test_cancelled_replacement_leaves_the_session_in_use(messenger, unique_subject):
    async with Reading(unique_subject, error_policy=FAST) as r:
        session = r.reader._session
        with ApiDown(r.reader, delay=30) as api:
            r.reader.reconnect()
            async with asyncio.timeout(10):
                while not api.attempts:
                    await asyncio.sleep(0.02)
            r.task.cancel()                 # in the middle of creating the new session
            await asyncio.gather(r.task, return_exceptions=True)
        assert r.reader._session is session
        r.task = asyncio.create_task(r._read())
        await r.publish_and_expect(1)


async def test_half_made_session_leaves_no_subscription_behind(messenger, unique_subject):
    """The consumer is created, asking about it fails: the subscription made for it is dropped."""
    async with Reading(unique_subject, error_policy=FAST) as r:
        js, nc = r.reader.connection.js, r.reader.connection.nc
        original = js.pull_subscribe
        made = []

        async def subscribe_then_fail_info(*args, **kwargs):
            subscription = await original(*args, **kwargs)
            made.append(subscription)

            async def no_info(*a, **k):
                raise nats.errors.TimeoutError

            subscription.consumer_info = no_info
            return subscription

        before = len(nc._subs)
        js.pull_subscribe = subscribe_then_fail_info
        try:
            with pytest.raises(Exception):
                await r.reader._replace_session()
        finally:
            js.pull_subscribe = original
        assert len(made) == 1
        assert len(nc._subs) == before
        await r.publish_and_expect(1)


@pytest.mark.parametrize('named_by', ['durable_name', 'name'])
async def test_reader_with_a_named_consumer_gets_a_new_session(messenger, unique_subject, named_by):
    """A durable consumer is its name: it is created anew, continuing after the last message read.

    `name` of the consumer configuration is ignored by the client library today - such a reader gets a
    consumer of its own for every session. Either way the reader ends with one working consumer.
    """
    name = f'{named_by}_' + unique_subject.rsplit('.', 1)[-1]
    async with Reading(unique_subject, error_policy=FAST, **{named_by: name}) as r:
        js, old = r.reader.connection.js, r.reader._session
        await r.reader._replace_session()
        new = r.reader._session
        assert new is not old
        info = await js.consumer_info(new.stream, new.consumer)       # raises if it is not there
        if new.consumer == old.consumer:                                # the same name: created anew, not bound
            assert new.consumer == name and named_by == 'durable_name'
            assert info.config.opt_start_seq == r.reader.last_seq + 1
        else:                                                           # another consumer: the old one is gone
            with pytest.raises(nats.js.errors.NotFoundError):
                await js.consumer_info(old.stream, old.consumer)
        await r.publish_and_expect(1)
        assert r.reader.health_status['reconnect_count'] == 1          # no second replacement was needed


async def test_durable_reader_loses_nothing_delivered_to_the_session_it_gave_up(messenger, unique_subject):
    """Messages the server delivered to the old session and nobody read come again, in order, before newer ones."""
    durable = 'durable_' + unique_subject.rsplit('.', 1)[-1]
    publisher = get_publisher(unique_subject)
    reader = get_reader(unique_subject, deliver_policy='all', error_policy=FAST, durable_name=durable)
    await publisher.open()
    await reader.open()
    try:
        for n in range(5):
            await publisher.publish(data={'n': n})
        assert (await reader.read_next())[0] == {'n': 0}
        old = reader.pull_subscription
        # the server delivers further messages to the session which is about to be given up
        await reader.connection.nc.publish(old._nms, b'{"batch": 2, "no_wait": true}', old._deliver)
        async with asyncio.timeout(5):
            while old._sub._pending_queue.qsize() < 2:
                await asyncio.sleep(0.02)
        await reader._replace_session()
        await publisher.publish(data={'n': 5})
        got = []
        async with asyncio.timeout(10):
            while len(got) < 5:
                got.append((await reader.read_next())[0]['n'])
        assert got == [1, 2, 3, 4, 5]
    finally:
        await reader.close()
        await publisher.close()


async def test_session_is_replaced_on_a_stream_with_no_spare_consumer(messenger):
    """No slot for "the new one first": the old consumer is given up, the reader reads on."""
    token = uuid.uuid4().hex[:8]
    js = messenger.connection.js
    await js.add_stream(name=f'full_{token}', subjects=[f'full.{token}.>'], storage='memory', max_consumers=1)
    try:
        async with Reading(f'full.{token}.x', error_policy=FAST) as r:
            old = r.reader._session
            await r.reader._replace_session()
            assert r.reader._session is not old
            await r.publish_and_expect(1)
            # ... also when the loop finds out by itself, and when it has to wait for the slot
            await r.reader._session.subscription.unsubscribe()
            await r.publish_and_expect(2)
    finally:
        await js.delete_stream(f'full_{token}')


async def test_consumer_which_could_not_be_given_up_stays_with_the_reader(messenger):
    """No spare consumer and the old one cannot be deleted: the reader keeps it, to try again - and to read on."""
    token = uuid.uuid4().hex[:8]
    js = messenger.connection.js
    await js.add_stream(name=f'full_{token}', subjects=[f'full.{token}.>'], storage='memory', max_consumers=1)
    original = js.delete_consumer
    try:
        async with Reading(f'full.{token}.x', error_policy=FAST) as r:
            old = r.reader._session

            async def times_out(*args, **kwargs):
                raise nats.errors.TimeoutError

            js.delete_consumer = times_out
            with pytest.raises(Exception, match='Deleting the consumer'):
                await r.reader._replace_session()
            assert r.reader._session is old and not old.gone
            await r.publish_and_expect(1)           # the session it kept is the one which works
            js.delete_consumer = original
            await r.reader._replace_session()
            assert r.reader._session is not old
            await r.publish_and_expect(2)
    finally:
        js.delete_consumer = original
        await js.delete_stream(f'full_{token}')


async def test_refused_pull_request_is_an_error_not_an_empty_subject(messenger, unique_subject):
    """A consumer limited to smaller requests than the reader makes: every pull is refused, and the reader says so."""
    reader = get_reader(unique_subject, deliver_policy='new', error_policy=ErrorPolicy(on_error=OnError.RAISE),
                        batch_policy=BatchPolicy(dynamic=False, max_batch=5))
    await reader.open()
    try:
        js = reader.connection.js
        info = await reader.pull_subscription.consumer_info()
        config = {k: v for k, v in info.config.as_dict().items() if v is not None} | {'max_batch': 1}
        await js._api_request(f'{js._prefix}.CONSUMER.CREATE.{info.stream_name}.{info.name}',
                              json.dumps({'stream_name': info.stream_name, 'config': config,
                                          'action': 'update'}).encode(), timeout=2)
        with pytest.raises(MessengerJetStreamError, match='Exceeded MaxRequestBatch') as refused:
            await reader.read_next()
        assert refused.value.code == 409
    finally:
        await reader.close()


async def test_messages_waiting_in_the_local_queue_are_delivered(messenger, unique_subject):
    """Messages delivered by an earlier pull are an answer: no new pull is sent, and no error may be made of that."""
    publisher = get_publisher(unique_subject)
    reader = get_reader(unique_subject, deliver_policy='all', error_policy=ErrorPolicy(on_error=OnError.RAISE))
    await publisher.open()
    await reader.open()
    try:
        for n in range(2):
            await publisher.publish(data={'n': n})
        subscription = reader.pull_subscription
        # a pull whose answer nobody collected (a wait given up, a cancelled read): both messages are queued locally
        await reader.connection.nc.publish(subscription._nms, b'{"batch": 2, "no_wait": true}', subscription._deliver)
        async with asyncio.timeout(5):
            while subscription._sub._pending_queue.qsize() < 2:
                await asyncio.sleep(0.02)
        got = [(await reader.read_next())[0]['n'] for _ in range(2)]
        assert got == [0, 1]
    finally:
        await reader.close()
        await publisher.close()


async def test_error_streak_ended_by_a_buffered_message(messenger, unique_subject):
    """A failure followed by a message already in the buffer: the message is returned, the failure is over."""
    publisher = get_publisher(unique_subject)
    reader = get_reader(unique_subject, deliver_policy='all',
                        error_policy=ErrorPolicy(on_error=OnError.WAIT,
                                                 retry=RetryPolicy(delay=0.05, max_delay=0.05, total_timeout=5.0)))
    await publisher.open()
    await reader.open()
    real_unpack = reader.messenger.unpack_nats_msg
    calls = []

    def malformed_once(msg):
        calls.append(1)
        if len(calls) == 1:
            raise ValueError('malformed message')
        return real_unpack(msg)

    try:
        for n in range(2):
            await publisher.publish(data={'n': n})
        reader.messenger.unpack_nats_msg = malformed_once
        data, meta = await asyncio.wait_for(reader.read_next(), timeout=5)
        assert data == {'n': 1}
        assert len(calls) == 2
    finally:
        del reader.messenger.unpack_nats_msg
        await reader.close()
        await publisher.close()


async def test_recovery_attempt_does_not_outlast_the_deadline(messenger, unique_subject):
    """total_timeout limits the attempt to recover, not only the pauses between attempts."""
    reader = get_reader(unique_subject, deliver_policy='new',
                        error_policy=ErrorPolicy(on_error=OnError.WAIT,
                                                 retry=RetryPolicy(delay=0.05, max_delay=0.05, total_timeout=1.0)))
    await reader.open()
    calls = []

    async def fails_then_hangs(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError('fetch failed')
        await asyncio.sleep(30)

    reader.fetch_available = fails_then_hangs
    loop = asyncio.get_running_loop()
    try:
        started = loop.time()
        with pytest.raises(RuntimeError, match='fetch failed'):
            await reader.read_next()
        assert 0.9 <= loop.time() - started < 1.5
    finally:
        del reader.fetch_available
        await reader.close()
