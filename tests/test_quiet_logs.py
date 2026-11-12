"""What serverish logs when the connection is in trouble (issues #46, #17, #51):
one clear line instead of a burst, no traceback for an expected outcome,
and an empty server is not a broken connection.
"""
from __future__ import annotations

import asyncio
import logging
import os
from unittest import mock

import pytest
from nats.js import JetStreamContext

from serverish.base import BurstLog, MessengerCannotConnect, Status
from serverish.connection.connection_jets import ConnectionJetStream
from serverish.messenger import Messenger, get_reader


class Records(logging.Handler):
    """Collects log records (independent of pytest's logging plugin)"""

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    def of_level(self, level: int, logger: str = 'test_burst') -> list[str]:
        """Messages of one logger: other tests' drivers may still be logging in the background"""
        return [r.getMessage() for r in self.records if r.levelno == level and r.name == logger]


@pytest.fixture
def records():
    handler = Records()
    root = logging.getLogger()
    previous = root.level
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    yield handler
    root.removeHandler(handler)
    root.setLevel(previous)


# ---------------------------------------------------------------------------
# BurstLog
# ---------------------------------------------------------------------------

async def test_burst_first_warning_in_full_the_rest_summed_up(records):
    burst = BurstLog(logging.getLogger('test_burst'), window=0.1)
    for i in range(100):
        burst.warning("Consumer has gone", f"reader {i}: consumer has gone")
    assert records.of_level(logging.WARNING) == ["reader 0: consumer has gone"]
    await asyncio.sleep(0.2)
    warnings = records.of_level(logging.WARNING)
    assert len(warnings) == 2
    assert warnings[1].startswith("Consumer has gone: 99 more such warning(s)")
    assert len(records.of_level(logging.DEBUG)) == 99  # nothing is lost, only moved to DEBUG


async def test_burst_kinds_are_independent_and_a_lone_warning_has_no_summary(records):
    burst = BurstLog(logging.getLogger('test_burst'), window=0.05)
    burst.warning("Consumer has gone", "a: consumer has gone")
    burst.warning("Reading failed, retrying", "a: read error")
    await asyncio.sleep(0.15)
    assert records.of_level(logging.WARNING) == ["a: consumer has gone", "a: read error"]
    burst.warning("Consumer has gone", "b: consumer has gone")  # a new window: in full again
    assert records.of_level(logging.WARNING)[-1] == "b: consumer has gone"


def test_burst_without_a_running_loop_sums_up_on_the_next_warning(records):
    burst = BurstLog(logging.getLogger('test_burst'), window=5.0)
    with mock.patch('serverish.base.burst_log.time.monotonic', side_effect=[0.0, 1.0, 10.0, 10.0]):
        burst.warning("Consumer has gone", "a")   # opens the window
        burst.warning("Consumer has gone", "b")   # within the window
        burst.warning("Consumer has gone", "c")   # after the window: summary of the old one, then in full
    warnings = records.of_level(logging.WARNING)
    assert warnings[0] == "a"
    assert warnings[1].startswith("Consumer has gone: 1 more such warning(s)")
    assert warnings[2] == "c"


# ---------------------------------------------------------------------------
# An empty server is not a broken connection (#51)
# ---------------------------------------------------------------------------

async def test_server_without_streams_is_a_warning_not_a_failure():
    connection = ConnectionJetStream('localhost', 4222)
    connection.js = mock.create_autospec(JetStreamContext, instance=True)
    connection.js.streams_info = mock.AsyncMock(return_value=[])
    report = await connection.diagnose_stream_exists()
    assert report.status is Status.WARNING


# ---------------------------------------------------------------------------
# Idle reader does not question the server every cycle (#46)
# ---------------------------------------------------------------------------

@pytest.mark.nats
@pytest.mark.timeout(30)
async def test_idle_reader_asks_about_its_consumer_only_periodically(messenger, unique_subject):
    reader = get_reader(unique_subject, deliver_policy='new')
    await reader.open()
    try:
        calls = 0
        original = reader.pull_subscription.consumer_info

        async def counted(*args, **kwargs):
            nonlocal calls
            calls += 1
            return await original(*args, **kwargs)

        with mock.patch.object(reader.pull_subscription, 'consumer_info', counted):
            reading = asyncio.ensure_future(reader.read_next())
            await asyncio.sleep(7)  # three empty 2 s wait cycles, below the 10 s health check interval
            reading.cancel()
            await asyncio.gather(reading, return_exceptions=True)
        assert calls == 0
    finally:
        await reader.close()


# ---------------------------------------------------------------------------
# Messenger.open() giving up (#17)
# ---------------------------------------------------------------------------

@pytest.mark.nats
@pytest.mark.timeout(30)
@pytest.mark.skipif(bool(os.getenv('CI')), reason='Singleton lifecycle test with failed connection is '
                                                 'timing-sensitive; nats reconnect_time_wait makes CI unreliable')
async def test_open_giving_up_logs_one_warning_and_no_traceback(nats_server, records):
    msg = Messenger()
    await msg.close()
    try:
        with pytest.raises(MessengerCannotConnect):
            await msg.open(host=nats_server['host'], port=4225, timeout=0.5)
        await asyncio.sleep(0.1)  # let the task callbacks log
        ours = [r for r in records.records if r.name in ('messenger', 'task_manager')]
        assert not [r for r in ours if r.levelno >= logging.ERROR]
        assert not [r for r in ours if r.exc_info]
        warnings = [r.getMessage() for r in ours if r.levelno == logging.WARNING]
        assert len(warnings) == 1 and 'giving up' in warnings[0]
        assert not [r for r in ours if 'background' in r.getMessage().lower()]
        # the failed attempt leaves the Messenger closed: open() works again without close()
        assert not msg.is_open
        await msg.open(host=nats_server['host'], port=nats_server['port'])
        assert msg.is_open
    finally:
        if not msg.is_open:
            await msg.close()
            await msg.open(host=nats_server['host'], port=nats_server['port'])
