"""Factory function for creating monitors with auto-detection.

Selects the appropriate MonitoredObject implementation based on
whether a Messenger instance is available and open.
"""

from __future__ import annotations

import os
import sys

from serverish.monitoring.monitored import DummyMonitoredObject, MonitoredObject
from serverish.monitoring.monitored_nats import MessengerMonitoredObject


async def create_monitor(
    name: str | None = None,
    *,
    subject_prefix: str = "svc",
    heartbeat_interval: float = 10.0,
    healthcheck_interval: float = 30.0,
    parent_name: str | None = None,
) -> MonitoredObject:
    """Factory function for creating monitored objects.

    Automatically selects implementation based on environment:
    - If Messenger singleton is available and open: MessengerMonitoredObject
    - Otherwise: DummyMonitoredObject (no-op)

    Args:
        name: Unique monitor name (used in NATS subjects: {prefix}.status.{name}).
              If None, generates unique name from executable name.
        subject_prefix: NATS subject prefix (default: 'svc')
        heartbeat_interval: Heartbeat interval in seconds (default: 10)
        healthcheck_interval: Healthcheck interval in seconds (default: 30)
        parent_name: Optional parent name for hierarchical grouping in displays

    Returns:
        MonitoredObject implementation appropriate for current environment

    Example::

        monitor = await create_monitor('my_app')
        async with monitor:
            monitor.set_status(Status.OK, 'Running')
            await do_work()
    """
    if name is None:
        from serverish.base.idmanger import gen_uid
        exename = os.path.basename(sys.argv[0]) if sys.argv and sys.argv[0] else "unknown"
        if exename.endswith(".py"):
            exename = exename[:-3]
        name = gen_uid(exename, length=8)

    try:
        from serverish.messenger import Messenger
        messenger = Messenger()
        if messenger.is_open:
            return MessengerMonitoredObject(
                name=name,
                messenger=messenger,
                check_interval=heartbeat_interval,
                healthcheck_interval=healthcheck_interval,
                subject_prefix=subject_prefix,
                parent_name=parent_name,
            )
    except Exception:
        pass

    return DummyMonitoredObject(name)
