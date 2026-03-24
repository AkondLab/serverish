"""Base MonitoredObject with hierarchical status aggregation.

To be extracted and generalized from ocabox-tcs MonitoredObject,
ReportingMonitoredObject, and MessengerMonitoredObject.

Key features:
    - Parent-child status aggregation
    - Periodic heartbeat and health check loops
    - Task tracking (BUSY/IDLE transitions)
    - Health check and metric callbacks
"""

from __future__ import annotations
