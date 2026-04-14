"""serverish.monitoring — Universal service monitoring over NATS.

Provides health checking, heartbeats, status aggregation, and service
lifecycle tracking for any serverish-based application.

Extracted from ocabox-tcs monitoring module and generalized to work
without TCS dependency.

Key components:
    Status          — Service status enum (OK, BUSY, ERROR, etc.)
    StatusReport    — Dataclass for status reports with serialization
    MonitoredObject — Base class for monitored entities with hierarchical
                      status aggregation and health check loops
    create_monitor  — Factory that auto-selects NATS vs dummy implementation

Example::

    from serverish.monitoring import create_monitor, Status

    monitor = await create_monitor('my_service')
    async with monitor:
        monitor.set_status(Status.OK, 'Running')
        async with monitor.track_task('processing'):
            await do_work()
"""

from serverish.monitoring.bridge import (
    bind_diagnostics,
    diagnostic_to_monitoring_status,
    diagnostics_to_status,
    health_status_metric_cb,
)
from serverish.monitoring.factory import create_monitor
from serverish.monitoring.monitored import (
    DummyMonitoredObject,
    MonitoredObject,
    ReportingMonitoredObject,
)
from serverish.monitoring.monitored_nats import MessengerMonitoredObject
from serverish.monitoring.status import Status, StatusReport, aggregate_status

__all__ = [
    "Status",
    "StatusReport",
    "aggregate_status",
    "MonitoredObject",
    "ReportingMonitoredObject",
    "MessengerMonitoredObject",
    "DummyMonitoredObject",
    "create_monitor",
    "bind_diagnostics",
    "diagnostics_to_status",
    "diagnostic_to_monitoring_status",
    "health_status_metric_cb",
]
