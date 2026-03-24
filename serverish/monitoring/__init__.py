"""serverish.monitoring — Universal service monitoring over NATS.

Provides health checking, heartbeats, status aggregation, and service
lifecycle tracking for any serverish-based application.

Extracted from ocabox-tcs monitoring module and generalized to work
without TCS dependency.

Key components:
    Status          — Service status enum (OK, BUSY, ERROR, etc.)
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

from __future__ import annotations
