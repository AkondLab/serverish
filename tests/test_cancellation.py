"""Cancellation must always win: a reader waiting for data ends when its task is cancelled."""
import asyncio
import warnings

import nats.errors
import pytest

from serverish.base import wait_for_psce
from serverish.messenger import get_reader


@pytest.mark.nats
async def test_read_next_honours_a_cancellation_lost_below_it(messenger, unique_subject):
    """serverish#32: a cancel swallowed in the fetch path must not leave read_next() polling for ever.

    The fetch below is the worst case seen in production: the cancellation
    delivered to it comes out as a plain timeout ("no message yet").
    """
    reader = get_reader(unique_subject, deliver_policy='new')
    await reader.open()
    swallowed = []

    async def fetch_losing_cancellation(*args, **kwargs):
        try:
            await asyncio.sleep(0.2)
        except asyncio.CancelledError:
            swallowed.append(1)
        raise nats.errors.TimeoutError

    reader.pull_subscription.fetch = fetch_losing_cancellation
    try:
        task = asyncio.create_task(reader.read_next())
        await asyncio.sleep(1.0)            # the reader is in its waiting loop
        task.cancel()
        done, pending = await asyncio.wait([task], timeout=5)
        assert done, 'read_next() kept running after its task was cancelled'
        assert task.cancelled()
        assert swallowed, 'the test did not exercise the lost-cancellation path'
    finally:
        del reader.pull_subscription.fetch
        await reader.close()


@pytest.mark.nats
async def test_read_next_under_timeout_returns_on_time(messenger, unique_subject):
    """The caller's pattern of serverish#32: read with a timeout on a silent subject."""
    reader = get_reader(unique_subject, deliver_policy='new')
    await reader.open()
    try:
        loop = asyncio.get_running_loop()
        started = loop.time()
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(2):
                await reader.read_next()
        assert loop.time() - started < 4
    finally:
        await reader.close()


async def test_wait_for_psce_shim_delivers_result_and_warns():
    async def work():
        return 42

    with pytest.warns(DeprecationWarning, match='asyncio.timeout'):
        assert await wait_for_psce(work(), timeout=1) == 42


async def test_wait_for_psce_shim_times_out_and_cancels_the_work():
    cancelled = asyncio.Event()

    async def work():
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    with warnings.catch_warnings():
        warnings.simplefilter('ignore', DeprecationWarning)
        with pytest.raises(TimeoutError):
            await wait_for_psce(work(), timeout=0.1)
    assert cancelled.is_set()


async def test_wait_for_psce_shim_propagates_the_callers_cancellation():
    cancelled = asyncio.Event()

    async def work():
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def caller():
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DeprecationWarning)
            await wait_for_psce(work(), timeout=10)

    task = asyncio.create_task(caller())
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled.is_set()
