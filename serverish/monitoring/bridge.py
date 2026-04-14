"""Bridge between serverish.base diagnostics and serverish.monitoring.

Connects the two status systems:

- ``serverish.base.Status`` / ``HasStatuses`` — low-level diagnostic checks
  (connection alive? messages flowing? DNS resolving?)

- ``serverish.monitoring.Status`` / ``MonitoredObject`` — service-level
  operational status (OK, BUSY, ERROR, etc.)

The bridge translates diagnostic results into monitoring status, and
exposes ``health_status`` dicts from publishers/readers/connections as
monitoring metrics.

Usage::

    from serverish.monitoring.bridge import (
        diagnostics_to_status,
        health_status_metric_cb,
        bind_diagnostics,
    )

    # Translate diagnostic results to monitoring status
    status = diagnostics_to_status(await connection.diagnose())

    # Register health_status dict as a metric source
    monitor.add_metric_cb(health_status_metric_cb(publisher))

    # Full integration: auto-update monitoring from HasStatuses diagnostics
    bind_diagnostics(monitor, connection)
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from serverish.base.status import Status as DiagStatus
from serverish.base.status import StatusEnum
from serverish.monitoring.status import Status as MonitoringStatus

logger = logging.getLogger(__name__)


def diagnostic_to_monitoring_status(diag_status: DiagStatus) -> MonitoringStatus:
    """Convert a single base.Status to monitoring.Status.

    Args:
        diag_status: A serverish.base.Status diagnostic result

    Returns:
        Corresponding monitoring.Status value
    """
    if diag_status.status == StatusEnum.ok:
        return MonitoringStatus.OK
    elif diag_status.status == StatusEnum.fail:
        return MonitoringStatus.ERROR
    elif diag_status.status == StatusEnum.disabled:
        return MonitoringStatus.SHUTDOWN
    elif diag_status.status == StatusEnum.na:
        return MonitoringStatus.UNKNOWN
    return MonitoringStatus.UNKNOWN


def diagnostics_to_status(results: dict[str, DiagStatus]) -> MonitoringStatus:
    """Convert HasStatuses.diagnose() results to a single monitoring status.

    Examines all diagnostic results and returns the worst status.

    Args:
        results: Dict from HasStatuses.diagnose() (e.g., {'ping': Status, 'dns': Status})

    Returns:
        Single monitoring.Status representing overall health
    """
    if not results:
        return MonitoringStatus.UNKNOWN

    has_fail = any(s.status == StatusEnum.fail for s in results.values())
    all_ok = all(s.status in (StatusEnum.ok, StatusEnum.na) for s in results.values())
    has_disabled = any(s.status == StatusEnum.disabled for s in results.values())

    if has_fail:
        return MonitoringStatus.ERROR
    if all_ok:
        return MonitoringStatus.OK
    if has_disabled:
        return MonitoringStatus.SHUTDOWN
    return MonitoringStatus.DEGRADED


def health_status_metric_cb(component) -> Callable[[], dict[str, Any]]:
    """Create a metric callback that reads health_status from a serverish component.

    Works with any object that has a ``health_status`` property returning a dict
    (MsgPublisher, MsgReader, ConnectionNATS, etc.).

    Usage::

        publisher = get_publisher('telemetry.weather')
        monitor.add_metric_cb(health_status_metric_cb(publisher))

    The health_status dict will be included in the monitor's status reports
    under ``details.metrics``.

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
    ``has_statuses.diagnose()`` and translates the result to a monitoring status.

    This means the monitor's periodic healthcheck loop will automatically
    verify the connection/component health and update accordingly.

    Usage::

        monitor = await create_monitor('my_service')
        bind_diagnostics(monitor, connection_nats)

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
            return MonitoringStatus.ERROR

    monitor.add_healthcheck_cb(_diagnose_check)
