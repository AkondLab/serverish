"""Deadlines: a publish never takes longer than its deadline, a reader never retries past its own."""
import asyncio

import nats.errors
import pytest

from serverish.base import (MessengerPublishAckTimeout, MessengerRequestTimeout, MessengerTimeout,
                            MessengerTransportTimeout, ServerishError)
from serverish.messenger import (ErrorPolicy, OnError, RetryPolicy, get_journalpublisher, get_publisher,
                                 get_reader)


class SilentServer:
    """Replaces JetStream publish of a publisher with one which never gets a confirmation"""

    def __init__(self, publisher):
        self.js = publisher.connection.js
        self.original = self.js.publish
        self.attempts: list[float] = []     # the time each attempt was given

    def __enter__(self):
        async def never_acked(subject, payload, timeout=None, **kwargs):
            self.attempts.append(timeout)
            await asyncio.sleep(timeout)
            raise nats.errors.TimeoutError

        self.js.publish = never_acked
        return self

    def __exit__(self, *exc):
        self.js.publish = self.original


def test_every_timeout_has_one_base():
    for cls in (MessengerPublishAckTimeout, MessengerTransportTimeout, MessengerRequestTimeout):
        assert issubclass(cls, MessengerTimeout)
    assert issubclass(MessengerTimeout, ServerishError)
    assert issubclass(MessengerTimeout, TimeoutError)


@pytest.mark.nats
async def test_publish_timeout_is_the_deadline_of_the_whole_call(messenger, unique_subject):
    """publish(timeout=X) returns or raises within X, retries included."""
    pub = get_publisher(unique_subject,
                        error_policy=ErrorPolicy(retry=RetryPolicy(attempt_timeout=0.4, delay=0.0)))
    await pub.open()
    loop = asyncio.get_running_loop()
    try:
        with SilentServer(pub) as server:
            started = loop.time()
            with pytest.raises(MessengerPublishAckTimeout, match='MAY have been delivered'):
                await pub.publish(data={'v': 1}, timeout=1.0)
            took = loop.time() - started
        assert 0.9 <= took < 1.3
        assert len(server.attempts) == 3                    # 0.4 + 0.4 + what was left
        assert server.attempts[-1] == pytest.approx(0.2, abs=0.1)
    finally:
        await pub.close()


@pytest.mark.nats
async def test_publish_deadline_comes_from_the_policy(messenger, unique_subject):
    """Without timeout= the deadline is RetryPolicy.total_timeout."""
    pub = get_publisher(unique_subject,
                        error_policy=ErrorPolicy(retry=RetryPolicy(total_timeout=0.6, attempt_timeout=0.25,
                                                                   delay=0.0)))
    await pub.open()
    loop = asyncio.get_running_loop()
    try:
        with SilentServer(pub):
            started = loop.time()
            with pytest.raises(MessengerTimeout):
                await pub.publish(data={'v': 1})
            assert 0.5 <= loop.time() - started < 0.9
    finally:
        await pub.close()


@pytest.mark.nats
async def test_publish_attempts_still_limit_the_retries(messenger, unique_subject):
    """A policy may stop earlier than the deadline."""
    pub = get_publisher(unique_subject,
                        error_policy=ErrorPolicy(retry=RetryPolicy(attempts=1, attempt_timeout=0.2,
                                                                   delay=0.0, total_timeout=30.0)))
    await pub.open()
    try:
        with SilentServer(pub) as server:
            with pytest.raises(MessengerPublishAckTimeout):
                await pub.publish(data={'v': 1})
        assert len(server.attempts) == 2                    # the first try and one retry
    finally:
        await pub.close()


@pytest.mark.nats
async def test_publish_which_fails_at_once_does_not_spin(messenger, unique_subject):
    """Default pacing: instant failures are spaced out, not repeated thousands of times."""
    pub = get_publisher(unique_subject)
    await pub.open()
    js = pub.connection.js
    original = js.publish
    calls = []

    async def fails_at_once(*args, **kwargs):
        calls.append(1)
        raise nats.errors.TimeoutError

    js.publish = fails_at_once
    try:
        with pytest.raises(MessengerPublishAckTimeout):
            await pub.publish(data={'v': 1}, timeout=1.0)
        assert 5 <= len(calls) <= 12
    finally:
        js.publish = original
        await pub.close()


@pytest.mark.nats
async def test_publish_is_stored_once_when_the_ack_is_late(messenger, unique_subject):
    """The happy path of a retry: the first attempt is stored, its ack lost, the retry deduplicated."""
    pub = get_publisher(unique_subject, error_policy=ErrorPolicy(retry=RetryPolicy(attempt_timeout=2.0)))
    await pub.open()
    await messenger.purge(unique_subject)
    js = pub.connection.js
    original = js.publish
    calls = []

    async def first_ack_lost(*args, **kwargs):
        calls.append(1)
        ack = await original(*args, **kwargs)
        if len(calls) == 1:
            raise nats.errors.TimeoutError
        return ack

    js.publish = first_ack_lost
    try:
        await pub.publish(data={'v': 1}, timeout=5.0)
    finally:
        js.publish = original
        await pub.close()
    assert len(calls) == 2
    reader = get_reader(unique_subject, deliver_policy='all', nowait=True)
    stored = [data async for data, meta in reader]
    assert stored == [{'v': 1}]


@pytest.mark.nats
async def test_journal_does_not_hold_the_program_up(messenger, unique_subject):
    """serverish#19: an unconfirmed journal entry is given up quickly and never raises."""
    journal = get_journalpublisher(unique_subject)
    await journal.open()
    loop = asyncio.get_running_loop()
    try:
        with SilentServer(journal):
            started = loop.time()
            await journal.info('on an unstable network', timeout=0.3)
            assert loop.time() - started < 0.6
        assert journal._ack_retry.total_timeout == 5.0      # the default without timeout=
    finally:
        await journal.close()


@pytest.mark.nats
async def test_reader_does_not_retry_after_its_deadline(messenger, unique_subject):
    """The pause before a retry ends at the deadline, and no retry starts there."""
    reader = get_reader(unique_subject, deliver_policy='new',
                        error_policy=ErrorPolicy(on_error=OnError.WAIT,
                                                 retry=RetryPolicy(delay=10.0, max_delay=10.0, total_timeout=0.5)))
    calls = []

    async def failing(*args, **kwargs):
        calls.append(1)
        raise RuntimeError('fetch failed')

    reader.fetch_available = failing
    loop = asyncio.get_running_loop()
    try:
        started = loop.time()
        with pytest.raises(RuntimeError, match='fetch failed'):
            await reader.read_next()
        assert 0.4 <= loop.time() - started < 1.0
        assert len(calls) == 1                              # no attempt at or after the deadline
    finally:
        del reader.fetch_available
        await reader.close()


@pytest.mark.nats
async def test_reader_error_streak_ends_when_the_server_answers(messenger, unique_subject):
    """Two failures separated by a recovery are two streaks, also when no message came between them."""
    reader = get_reader(unique_subject, deliver_policy='new',
                        error_policy=ErrorPolicy(on_error=OnError.WAIT,
                                                 retry=RetryPolicy(delay=0.05, max_delay=0.05, total_timeout=1.0)))
    await reader.open()
    real_fetch_available = reader.fetch_available
    real_wait = reader.pull_subscription.fetch
    pulls, waits = [], []

    async def flaky_pull(*args, **kwargs):
        pulls.append(1)
        if len(pulls) in (1, 3):                            # fails, recovers, fails again, recovers
            raise RuntimeError('fetch failed')
        return await real_fetch_available(*args, **kwargs)

    async def wait_interrupted_once(*args, **kwargs):
        waits.append(1)
        if len(waits) == 1:
            await asyncio.sleep(1.3)                        # silence, longer than the budget of one streak
            raise RuntimeError('waiting interrupted')       # sends the reader back to pull again
        return await real_wait(*args, **kwargs)

    reader.fetch_available = flaky_pull
    reader.pull_subscription.fetch = wait_interrupted_once
    pub = get_publisher(unique_subject)
    await pub.open()
    try:
        task = asyncio.create_task(reader.read_next())
        await asyncio.sleep(2.5)
        assert not task.done(), 'the second failure was charged to the budget of the first one'
        assert len(pulls) >= 4
        await pub.publish(data={'v': 1})
        data, meta = await asyncio.wait_for(task, timeout=5)
        assert data == {'v': 1}
    finally:
        del reader.fetch_available
        await reader.close()
        await pub.close()
