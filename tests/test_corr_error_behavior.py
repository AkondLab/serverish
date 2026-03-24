"""Correctness tests for MsgReader error_behavior modes (CORR-03).

Tests all 3 error_behavior modes: RAISE, FINISH, and WAIT.

Error trigger approach used: **Invalidate pull subscription object**.
After opening a reader on a valid ``test.*`` subject and confirming the reader
is functional, we replace the internal ``pull_subscription`` attribute with a
broken proxy object whose methods raise exceptions.  This causes an immediate
``ErrorException`` inside the ``read_next()`` while-loop — exactly where the
``error_behavior`` switch lives.

This approach guarantees the error occurs DURING iteration (not during
``open()``) and avoids long network timeouts that plague approaches based on
deleting the consumer externally.
"""
from __future__ import annotations

import asyncio
import logging

import pytest

from serverish.messenger import Messenger, get_reader

logger = logging.getLogger(__name__)


class _BrokenSubscription:
    """A proxy that raises on any attribute access used by MsgReader.

    Simulates a fatally broken subscription — any operation on it
    (consumer_info, fetch, unsubscribe, attribute access for internals)
    raises a RuntimeError that propagates through @async_shield as ErrorException.
    """

    def __init__(self, error_message: str = 'subscription invalidated for testing') -> None:
        self._error_message = error_message

    def __getattr__(self, name: str):
        raise RuntimeError(self._error_message)


async def _sabotage_reader(reader) -> None:
    """Replace the pull subscription with a broken proxy to trigger ErrorException.

    First cleanly unsubscribes and deletes the real consumer, then injects
    the broken proxy so the next read_next() loop iteration fails immediately.
    """
    try:
        ci = await reader.pull_subscription.consumer_info()
        # Clean up the real consumer
        js = reader.connection.js
        await js.delete_consumer(stream=ci.stream_name, consumer=ci.name)
    except Exception:
        pass
    try:
        await reader.pull_subscription.unsubscribe()
    except Exception:
        pass
    # Replace with broken proxy — this triggers immediate ErrorException
    reader.pull_subscription = _BrokenSubscription()
    logger.info('Replaced pull_subscription with broken proxy')


@pytest.mark.nats
@pytest.mark.asyncio(loop_scope='session')
async def test_error_behavior_raise(messenger: Messenger, unique_subject: str) -> None:
    """RAISE mode re-raises the underlying error when iteration encounters an error."""
    reader = get_reader(unique_subject, deliver_policy='new', nowait=False, error_behavior='RAISE')
    try:
        await reader.open()
        await _sabotage_reader(reader)

        # Iterate — should raise the original RuntimeError (NOT MessengerReaderStopped)
        with pytest.raises(RuntimeError, match='subscription invalidated'):
            async for _data, _meta in reader:
                pass  # pragma: no cover — error expected before any message

        # Verify error was tracked
        assert reader._last_error is not None, 'RAISE mode should track the error in _last_error'
        logger.info('RAISE mode raised: %s', reader._last_error)

    finally:
        # Restore a None subscription so close() does not fail on the proxy
        reader.pull_subscription = None
        try:
            await reader.close()
        except Exception:
            pass


@pytest.mark.nats
@pytest.mark.asyncio(loop_scope='session')
async def test_error_behavior_finish(messenger: Messenger, unique_subject: str) -> None:
    """FINISH mode ends iteration silently (StopAsyncIteration) when an error occurs."""
    reader = get_reader(unique_subject, deliver_policy='new', nowait=False, error_behavior='FINISH')
    try:
        await reader.open()
        await _sabotage_reader(reader)

        # Iterate — the async-for loop should complete without raising
        collected: list[tuple[dict, dict]] = []
        async for data, meta in reader:
            collected.append((data, meta))  # pragma: no cover — error expected before messages

        # If we reach here, the loop ended normally via StopAsyncIteration
        # (which is exactly what FINISH mode should produce)
        logger.info('FINISH mode ended iteration silently, collected %d messages', len(collected))
        # Verify error was tracked
        assert reader._last_error is not None, 'FINISH mode should track the error in _last_error'

    finally:
        reader.pull_subscription = None
        try:
            await reader.close()
        except Exception:
            pass


@pytest.mark.nats
@pytest.mark.asyncio(loop_scope='session')
async def test_error_behavior_wait(messenger: Messenger, unique_subject: str) -> None:
    """WAIT mode retries with backoff instead of raising immediately.

    We verify WAIT behaviour by observing that the reader does NOT raise
    and does NOT finish within a short timeout.  Instead it keeps sleeping
    and retrying (the backoff formula is ``min(0.2 + n/5.0, 15.0)``).
    A 3-second timeout is enough to prove the retry loop is running
    (first retry waits 0.2s, second 0.4s, third 0.6s, etc.).
    """
    reader = get_reader(unique_subject, deliver_policy='new', nowait=False, error_behavior='WAIT')
    reader_task: asyncio.Task | None = None
    try:
        await reader.open()
        await _sabotage_reader(reader)

        # Run reader iteration in a background task
        error_caught: list[Exception] = []

        async def _iterate() -> None:
            try:
                async for _data, _meta in reader:
                    pass  # pragma: no cover
            except Exception as exc:
                error_caught.append(exc)

        reader_task = asyncio.create_task(_iterate())

        # Wait up to 3 seconds — WAIT mode should NOT finish in this time
        # (it retries with backoff sleep rather than raising or finishing)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(reader_task), timeout=3.0)

        # If we got TimeoutError, the reader is still retrying — correct WAIT behaviour
        logger.info('WAIT mode is retrying (did not raise or finish within 3s)')

        # Verify no exception leaked from the reader
        assert not error_caught, (
            f'WAIT mode should not raise, but got: {error_caught[0]!r}'
        )
        # Verify error was tracked
        assert reader._last_error is not None, 'WAIT mode should track the error in _last_error'

    finally:
        if reader_task is not None and not reader_task.done():
            reader_task.cancel()
            try:
                await reader_task
            except (asyncio.CancelledError, Exception):
                pass
        reader.pull_subscription = None
        try:
            await reader.close()
        except Exception:
            pass
