# Service Monitoring in Serverish

## Overview

Serverish provides two complementary status/health systems at different abstraction levels:

**Infrastructure diagnostics** (`serverish.base`) — "Are my connections working?"
Low-level, technical, per-component. Checks if NATS is connected, DNS resolves, messages are flowing.

**Service monitoring** (`serverish.monitoring`) — "What is my service doing?"
High-level, operational, service-wide. Tracks service status (OK, BUSY, ERROR), sends heartbeats, aggregates children, reports metrics.

A bridge module connects both systems so a single `MonitoredObject` can automatically reflect infrastructure health while tracking service-level state.

## Architecture

```
                        ┌───────────────────────────────────┐
                        │  Your Service Code                │
                        │                                   │
                        │  async with monitor.track_task(): │
                        │      await process_messages()     │
                        └──────────┬────────────────────────┘
                                   │
                    ┌──────────────▼────────────────────┐
                    │  MonitoredObject                  │
                    │  ┌──────────────────────────────┐ │
                    │  │ Status: OK / BUSY / ERROR    │ │
                    │  │ Children: [child1, child2]   │ │
                    │  │ Healthcheck callbacks        │ │
                    │  │ Metric callbacks             │ │
                    │  └──────────────────────────────┘ │
                    └──────┬───────────────┬────────────┘
                           │               │
              ┌────────────▼───┐    ┌──────▼──────────────┐
              │  Bridge        │    │  NATS Publishing    │
              │                │    │                     │
              │  diagnose() ──►│    │  svc.status.<name>  │
              │  health_status │    │  svc.heartbeat.<name│
              └───────┬────────┘    └─────────────────────┘
                      │
         ┌────────────▼───────────────┐
         │  serverish.base            │
         │  HasStatuses / Connection  │
         │  Publisher.health_status   │
         │  Reader.health_status      │
         └────────────────────────────┘
```

## Quick Start

### Minimal: status tracking without NATS

```python
from serverish.monitoring import MonitoredObject, Status

monitor = MonitoredObject("my_worker")
monitor.set_status(Status.OK, "Ready")

# Track active work
async with monitor.track_task("processing_batch"):
    # Status automatically: BUSY while inside, IDLE after 1s delay
    await process_batch()

# Check current state
report = await monitor.get_full_report()
print(report.status, report.message)
```

### Standard: monitoring with NATS heartbeats

```python
from serverish.messenger import Messenger
from serverish.monitoring import create_monitor, Status

async def main():
    messenger = Messenger()
    async with messenger.context(host='localhost', port=4222):
        # create_monitor auto-detects NATS and selects implementation
        monitor = await create_monitor('weather_collector')

        async with monitor:
            monitor.set_status(Status.OK, "Collecting data")

            # Heartbeats sent automatically every 10s
            # Status updates sent on every status change
            while running:
                async with monitor.track_task("poll_sensors"):
                    data = await poll_sensors()
                    await publish(data)
```

### Full integration: bridge to infrastructure diagnostics

```python
from serverish.messenger import Messenger, get_publisher, get_reader
from serverish.monitoring import (
    create_monitor, Status,
    bind_diagnostics, health_status_metric_cb,
)

async def main():
    messenger = Messenger()
    async with messenger.context(host='localhost', port=4222):
        monitor = await create_monitor('data_pipeline')
        publisher = get_publisher('telemetry.weather')
        reader = get_reader('raw.sensors')

        # Bridge: infrastructure diagnostics → monitoring healthcheck
        bind_diagnostics(monitor, messenger.conn)

        # Bridge: publisher/reader health → monitoring metrics
        monitor.add_metric_cb(health_status_metric_cb(publisher))
        monitor.add_metric_cb(health_status_metric_cb(reader))

        async with monitor:
            monitor.set_status(Status.OK, "Pipeline running")
            # Now the monitor:
            # - Sends heartbeats every 10s
            # - Runs healthcheck every 30s (including NATS diagnostics)
            # - Includes publisher/reader stats in status reports
            # - Tracks BUSY/IDLE based on task execution
            await run_pipeline()
```

## Status Enum

```
UNKNOWN   — Initial state, not yet determined
STARTUP   — Service is starting up (operational but not ready)
OK        — Healthy and ready
IDLE      — Healthy, no active tasks (requires task tracking)
BUSY      — Healthy, processing tasks (requires task tracking)
DEGRADED  — Operational but impaired (is_healthy=True)
WARNING   — Operational with warning conditions (is_healthy=True)
ERROR     — Not operational, error state
SHUTDOWN  — Shutting down gracefully
FAILED    — Not operational, hard failure
```

Properties:
- `status.is_healthy` — True for OK, IDLE, BUSY, DEGRADED, WARNING
- `status.is_operational` — True for all of the above plus STARTUP

Aggregation precedence (highest wins):
FAILED > ERROR > WARNING > DEGRADED > STARTUP > SHUTDOWN > BUSY > IDLE > OK

## Hierarchical Monitoring

Services can have child monitors. A parent's aggregated status reflects the worst child:

```python
pipeline = MonitoredObject("pipeline")
ingester = MonitoredObject("ingester", parent=pipeline)
processor = MonitoredObject("processor", parent=pipeline)

ingester.set_status(Status.OK)
processor.set_status(Status.ERROR, "Out of memory")

report = await pipeline.get_full_report()
# report.status == Status.ERROR (aggregated from children)
# report.details["own_status"] == "ok" (pipeline itself is fine)
# report.details["children"] == [ingester_report, processor_report]
```

## Task Tracking

Task tracking enables automatic BUSY/IDLE transitions:

```python
monitor.set_status(Status.OK)

async with monitor.track_task("batch_1"):
    # Status: BUSY (immediately)
    await process()
# Status: IDLE (after 1s delay — if no new task starts)
```

Key behaviors:
- First `track_task()` call enables task tracking permanently
- Overlapping tasks: stays BUSY until all complete
- 1s delay before IDLE prevents flicker between rapid tasks
- ERROR/FAILED status is NOT overridden by task start (intentional)
- Supports both `async with` and `with` (sync version schedules on running loop)

## Health Checks

Register callbacks for periodic verification:

```python
# Sync callback — return None (healthy) or Status (unhealthy)
def check_disk():
    if disk_usage() > 90:
        return Status.WARNING
    return None

monitor.add_healthcheck_cb(check_disk)

# Async callback
async def check_upstream():
    if not await upstream.ping():
        return Status.ERROR
    return None

monitor.add_healthcheck_cb(check_upstream)
```

Health checks run every 30s (configurable via `healthcheck_interval`).

## Metric Callbacks

Register callbacks to include custom metrics in status reports:

```python
monitor.add_metric_cb(lambda: {
    "queue_depth": len(queue),
    "processed_total": counter,
})

# Or use the bridge to include health_status from serverish components
monitor.add_metric_cb(health_status_metric_cb(publisher))
```

Metrics appear in `report.details["metrics"]`.

## NATS Publishing

When using `MessengerMonitoredObject` (created by `create_monitor()` when Messenger is open):

**Status updates** → `svc.status.<name>` (on every status change)
```json
{
    "name": "weather_collector",
    "status": "ok",
    "message": "Collecting data",
    "timestamp": [2025, 3, 25, 10, 30, 0, 0],
    "parent": "launcher.server01",
    "pid": 12345,
    "hostname": "server01.lan",
    "details": {
        "own_status": "ok",
        "children": [...],
        "metrics": {"publish_count": 42, "pending_messages": 0}
    }
}
```

**Heartbeats** → `svc.heartbeat.<name>` (every 10s, configurable)
```json
{
    "service_id": "weather_collector",
    "timestamp": [2025, 3, 25, 10, 30, 0, 0],
    "status": "ok"
}
```

## Bridge: Connecting Infrastructure to Service Monitoring

Serverish has two status systems — the bridge connects them:

| Function | What it does |
|----------|-------------|
| `bind_diagnostics(monitor, connection)` | Runs `connection.diagnose()` in healthcheck loop, translates fail→ERROR |
| `health_status_metric_cb(component)` | Wraps `component.health_status` dict as a metric callback |
| `diagnostics_to_status(results)` | Converts `HasStatuses.diagnose()` dict to single `monitoring.Status` |
| `diagnostic_to_monitoring_status(diag)` | Converts single `base.Status` to `monitoring.Status` |

Translation table:

| `base.StatusEnum` | `monitoring.Status` |
|-------------------|---------------------|
| ok | OK |
| fail | ERROR |
| disabled | SHUTDOWN |
| na | UNKNOWN |

## Graceful Degradation

If NATS is not available, `create_monitor()` returns a `DummyMonitoredObject` that provides the full API but doesn't publish anything. Your service code works identically — no conditional logic needed.

```python
# This works with or without NATS
monitor = await create_monitor('my_service')
async with monitor:
    monitor.set_status(Status.OK)
    async with monitor.track_task('work'):
        await do_work()
```

## Configuration

```python
monitor = await create_monitor(
    'my_service',
    subject_prefix='svc',         # NATS subject prefix (default: 'svc')
    heartbeat_interval=10.0,      # Seconds between heartbeats (default: 10)
    healthcheck_interval=30.0,    # Seconds between health checks (default: 30)
    parent_name='launcher.host1', # Parent name for hierarchical display
)
```

## TCS Compatibility

The monitoring module is extracted from `ocabox-tcs`. Class names, method signatures, and constructor arguments are identical, so TCS can switch to importing from `serverish.monitoring` with minimal changes:

```python
# Before (in TCS)
from ocabox_tcs.monitoring import create_monitor, Status, MonitoredObject

# After (using serverish)
from serverish.monitoring import create_monitor, Status, MonitoredObject
```

The NATS subject structure (`svc.status.*`, `svc.heartbeat.*`) matches TCS conventions, so `tcsctl` and other consumers work without modification.
