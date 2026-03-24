"""Resilience tests: RPC responder resubscription after NATS reconnect (RESL-03).

Proves that MsgRpcResponder automatically resubscribes to its subject
after NATS container pause/unpause, and handles new RPC requests
after reconnection.

Note: RPC uses core NATS (not JetStream), so subjects must NOT match
the test.> stream wildcard.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid

import pytest

from serverish.messenger import Messenger, get_rpcresponder, get_rpcrequester
from tests.conftest import wait_for_healthy

logger = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.nats,
    pytest.mark.nats_resilience,
    pytest.mark.timeout(60),
]


async def _force_disconnect_detection(messenger: Messenger, timeout: float = 15.0) -> None:
    """Force the NATS client to detect a broken connection.

    A paused container freezes TCP -- the client cannot detect this passively.
    We attempt a flush with a short timeout to trigger detection, then poll
    is_connected as the nats-py client processes the disconnect asynchronously.
    """
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        try:
            await asyncio.wait_for(
                messenger.connection.nc.flush(),
                timeout=2.0,
            )
            await asyncio.sleep(0.5)
        except Exception:
            logger.info('Flush failed -- disconnect triggered')
            break
    while time.monotonic() - start < timeout:
        if not messenger.connection.health_status['is_connected']:
            logger.info('Connection now reports disconnected')
            return
        await asyncio.sleep(0.3)
    logger.warning(
        'Could not confirm is_connected=False within timeout; proceeding with recovery phase'
    )


async def _poll_until_connected(messenger: Messenger, timeout: float = 20.0) -> None:
    """Poll until the NATS connection reports as connected again."""
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        status = messenger.connection.health_status
        if status['is_connected']:
            return
        await asyncio.sleep(0.3)
    raise TimeoutError('Connection did not reconnect within timeout')


async def test_rpc_responder_resubscribes_after_pause(resilience_messenger, nats_disruptor):
    """RPC responder handles new requests after NATS pause/unpause (RESL-03).

    Six-step pattern per D-07:
    1. BASELINE: verify normal RPC request/response
    2. DISRUPT: pause container
    3. VERIFY DEGRADED: detect disconnect
    4. RESTORE: unpause container
    5. VERIFY RECOVERY: RPC request succeeds after reconnect
    6. VERIFY METRICS: reconnect_count, has_subscription
    """
    m = resilience_messenger
    rpc_subject = f'rpc.resl.{uuid.uuid4().hex[:8]}'

    responder = get_rpcresponder(subject=rpc_subject)
    requester = get_rpcrequester(subject=rpc_subject)
    try:
        await responder.open()

        async def handler(rpc):
            rpc.set_response(data={'echo': rpc.data.get('msg')})

        await responder.register_function(handler)

        # --- 1. BASELINE ---
        rdata, rmeta = await asyncio.wait_for(
            requester.request(data={'msg': 'baseline'}, timeout=10),
            timeout=15,
        )
        assert rdata['echo'] == 'baseline'
        logger.info('BASELINE passed: RPC round-trip works normally')

        # --- 2. DISRUPT ---
        logger.info('DISRUPTING: pausing NATS container')
        nats_disruptor.pause()

        # --- 3. VERIFY DEGRADED ---
        await _force_disconnect_detection(m, timeout=15)
        conn_status = m.connection.health_status
        if conn_status['is_connected']:
            logger.warning('Connection still reports connected during pause (async detection lag)')
        else:
            logger.info('DEGRADED verified: connection reports disconnected')

        # --- 4. RESTORE ---
        logger.info('RESTORING: unpausing NATS container')
        nats_disruptor.unpause()

        # --- 5. VERIFY RECOVERY ---
        await _poll_until_connected(m, timeout=20)

        # Allow time for the reconnect callback to fire and resubscribe
        await asyncio.sleep(1.0)

        rdata, rmeta = await asyncio.wait_for(
            requester.request(data={'msg': 'after_reconnect'}, timeout=10),
            timeout=20,
        )
        assert rdata['echo'] == 'after_reconnect'
        logger.info('RECOVERY verified: RPC request succeeded after reconnect')

        # --- 6. VERIFY METRICS ---
        status = responder.health_status
        assert status['reconnect_count'] >= 1, (
            f'Expected reconnect_count >= 1, got: {status["reconnect_count"]}'
        )
        assert status['has_subscription'] is True
        assert status['is_open'] is True
        logger.info(
            'METRICS verified: reconnect_count=%d, has_subscription=%s',
            status['reconnect_count'], status['has_subscription'],
        )

    finally:
        await responder.close()
        await requester.close()


async def test_rpc_responder_health_during_disconnect(resilience_messenger, nats_disruptor):
    """RPC responder health_status transitions during disconnect (RESL-03 supplement).

    Lighter test focusing on health_status field transitions:
    1. Open responder -- verify initial healthy state
    2. Pause/unpause -- verify reconnect_count incremented
    3. Verify has_subscription restored after recovery
    """
    m = resilience_messenger
    rpc_subject = f'rpc.resl.health.{uuid.uuid4().hex[:8]}'

    responder = get_rpcresponder(subject=rpc_subject)
    try:
        await responder.open()

        async def handler(rpc):
            rpc.set_response(data={'ok': True})

        await responder.register_function(handler)

        # --- 1. Initial healthy state ---
        pre = responder.health_status
        assert pre['has_subscription'] is True
        assert pre['reconnect_count'] == 0
        assert pre['is_open'] is True
        logger.info('Initial state verified: has_subscription=True, reconnect_count=0')

        # --- 2. Disrupt and restore ---
        nats_disruptor.pause()
        await _force_disconnect_detection(m, timeout=15)
        nats_disruptor.unpause()

        # --- 3. Wait for reconnect to complete ---
        start = time.monotonic()
        while time.monotonic() - start < 20:
            status = responder.health_status
            if status['reconnect_count'] >= 1:
                break
            await asyncio.sleep(0.5)
        else:
            pytest.fail(
                f'reconnect_count did not reach >= 1 within 20s. '
                f'Last status: {responder.health_status}'
            )

        # --- 4. Verify recovered state ---
        post = responder.health_status
        assert post['has_subscription'] is True, (
            f'Expected has_subscription=True after recovery, got: {post}'
        )
        assert post['reconnect_count'] >= 1
        logger.info(
            'Recovery verified: reconnect_count=%d, has_subscription=%s',
            post['reconnect_count'], post['has_subscription'],
        )

    finally:
        await responder.close()
