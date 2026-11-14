"""The consumer session of a reader: whatever fails around it, an open reader reads again by itself.

The cases come from the observatory outage of 2026-11-14, where readers of a long-running
service stayed open and deaf for hours after the network was back.
"""
import asyncio

import nats.errors
import nats.js.errors
import pytest

from serverish.base import MessengerReaderConfigError
from serverish.connection.nats_errors import is_missing_stream
from serverish.messenger import ErrorPolicy, OnError, RetryPolicy, get_publisher, get_reader

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
