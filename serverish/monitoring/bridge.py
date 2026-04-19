"""Bridge between serverish.base diagnostics and serverish.monitoring.

Connects HasStatuses diagnostic results to MonitoredObject healthcheck
callbacks. With the unified status types, no translation is needed —
both systems now use the same Status enum and StatusReport dataclass.

Usage::

    from serverish.monitoring.bridge import (
        bind_diagnostics,
        health_status_metric_cb,
    )

    # Register HasStatuses diagnostics as monitoring healthcheck
    bind_diagnostics(monitor, connection)

    # Register health_status dict as a metric source
    monitor.add_metric_cb(health_status_metric_cb(publisher))
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from serverish.base.status import Status, StatusReport

logger = logging.getLogger(__name__)


def diagnostics_to_status(results: dict[str, StatusReport]) -> Status:
    """Convert HasStatuses.diagnose() results to a single Status.

    Examines all diagnostic results and returns the worst status.

    Args:
        results: Dict from HasStatuses.diagnose()

    Returns:
        Single Status representing overall health
    """
    if not results:
        return Status.UNKNOWN

    has_error = any(
        r.status in (Status.ERROR, Status.FAILED) for r in results.values()
    )
    all_healthy = all(
        r.status.is_healthy or r.status == Status.UNKNOWN for r in results.values()
    )
    has_shutdown = any(r.status == Status.SHUTDOWN for r in results.values())

    if has_error:
        return Status.ERROR
    if all_healthy:
        return Status.OK
    if has_shutdown:
        return Status.SHUTDOWN
    return Status.DEGRADED


def health_status_metric_cb(component) -> Callable[[], dict[str, Any]]:
    """Create a metric callback that reads health_status from a serverish component.

    Works with any object that has a ``health_status`` property returning a dict
    (MsgPublisher, MsgReader, ConnectionNATS, etc.).

    Usage::

        publisher = get_publisher('telemetry.weather')
        monitor.add_metric_cb(health_status_metric_cb(publisher))

    Args:
        component: Any serverish component with a health_status property

    Returns:
        Callable that returns the health_status dict
    """
    def _get_health_status() -> dict[str, Any]:
        try:
            return component.health_status
        except Exception as e:
            logger.warning(f"Failed to read health_status from {component}: {e}")
            return {}

    return _get_health_status


def bind_diagnostics(monitor, has_statuses) -> None:
    """Bind a HasStatuses component to a MonitoredObject.

    Registers an async healthcheck callback on the monitor that runs
    ``has_statuses.diagnose()`` and evaluates the overall health.

    Args:
        monitor: A MonitoredObject instance
        has_statuses: A HasStatuses instance (ConnectionNATS, etc.)
    """
    async def _diagnose_check():
        try:
            results = await has_statuses.diagnose()
            status = diagnostics_to_status(results)
            if not status.is_healthy:
                return status
            return None  # Healthy — don't override current status
        except Exception as e:
            logger.warning(f"Diagnostic check failed for {has_statuses}: {e}")
            return Status.ERROR

    monitor.add_healthcheck_cb(_diagnose_check)
