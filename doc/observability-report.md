# Observability in Serverish Ecosystem

**Report: Serverish / TCS / PMS and OpenTelemetry**
Date: 2025-03-24

---

## 1. Executive Summary

The OCM observatory runs ~14 Python services, all built on **serverish** as the messaging and component foundation, communicating over **NATS JetStream**. Observability today is split between two custom layers:

1. **Serverish** — low-level health diagnostics, message tracing, connection monitoring
2. **TCS (ocabox-tcs)** — high-level service lifecycle, heartbeats, hierarchical status aggregation, `tcsctl` CLI

**OpenTelemetry** has been introduced experimentally in PMS (aggregator module) but is not yet integrated into the shared infrastructure.

**This report proposes** a unified observability strategy where serverish provides the foundational observability primitives — usable by any Python service, whether TCS-managed or standalone — and optionally bridges to OpenTelemetry for integration with industry-standard backends (Grafana, Jaeger, etc.).

---

## 2. Current State: Who Uses What

### 2.1 Projects Using Serverish (14 confirmed)

| Project | Role | Serverish Usage | Observability Today |
|---------|------|-----------------|---------------------|
| **ocabox-server** | TIC middleware | Messenger, pub/sub | Connection diagnostics |
| **ocabox-tcs** | Service framework | Messenger, pub/sub | Full monitoring framework (see §3) |
| **pms** | Metrics puller | Publisher, reader, callbacks | OpenTelemetry traces (experimental) |
| **halina** | Autonomous controller | Messenger patterns | Minimal |
| **halina9000** | AI assistant | Messenger, RPC | Minimal |
| **TOI** | Operator GUI | Messenger, reader | Status display |
| **oca_monitor** | Dashboard GUI | Messenger, reader | Real-time NATS visualization |
| **ocabox** | Client library | Messenger | Connection status |
| **ocabox-cli** | CLI tool | Messenger | Connection diagnostics |
| **oca-data-to-nats** | Weather ingestion | Publisher | Minimal |
| **oca-fits-proc** | FITS pipeline | Messenger | Minimal |
| **oca_nats_config** | NATS config | Infrastructure | N/A |
| **ocadb** | Observation DB | Messenger | Minimal |
| **oca-graylog** | Log integration | Stub | None |

### 2.2 Potential Future Adopters

- **ofdb** — planned observation database with NATS ingestion
- **teda** — FITS viewer (could add telemetry)
- **oca-dockers** — container orchestration (could add service monitoring)
- Any new Python service in the observatory ecosystem

---

## 3. Observability Layers — Detailed Analysis

### 3.1 Serverish Layer (Foundation)

**What exists today:**

| Feature | Location | Description |
|---------|----------|-------------|
| **HasStatuses** | `base/hasstatuses.py` | Ordered diagnostic checks with deduction (if ping OK → skip DNS) |
| **Connection health** | `connection/connection_nats.py` | Slow consumer tracking, error counts, reconnect callbacks, `health_status` property |
| **Publisher health** | `messenger/msg_publisher.py` | Publish count, error count, last publish time, `health_status` property |
| **Reader health** | `messenger/msg_reader.py` | Message count, reconnect count, pending queue depth, duplicate detection, `health_status` property |
| **Message tracing** | `messenger/messenger.py` | Per-message trace levels, `log_msg_trace()`, `msg_to_repr()` |
| **Schema validation** | `messenger/msgvalidator.py` | JSON Schema enforcement on all messages |
| **Journal logging** | `messenger/msg_journal_pub.py` | Structured logging over NATS (debug→critical), `NatsJournalLoggingHandler` bridge |
| **Progress tracking** | `messenger/msg_progress_pub.py` | Task progress reporting over NATS |
| **Live documents** | `messenger/live_document.py` | Auto-updating config with change callbacks and version tracking |

**What's missing:**

- No structured metrics export (counters exist but aren't exposed to external systems)
- No distributed tracing (trace_level is per-message logging, not span-based)
- No service registry or discovery at this layer
- No standard health endpoint (each component has `health_status` but no aggregation)
- Health data is in-process only — no way to query from outside

### 3.2 TCS Layer (Service Management)

**Built on top of serverish**, TCS adds:

| Feature | Description |
|---------|-------------|
| **MonitoredObject** | Base class for any monitored entity with hierarchical status aggregation |
| **Heartbeats** | Periodic signals over NATS (`svc.heartbeat.<name>`, default 10s) |
| **Status publishing** | Status changes broadcast over NATS (`svc.status.<name>`) with child statuses and metrics |
| **Service registry** | Lifecycle events (declared→start→ready→stopping→stop) on `svc_registry` stream |
| **Crash detection** | Missing heartbeat → stale → dead classification; exit code tracking |
| **Task tracking** | `async with monitor.track_task()` for BUSY/IDLE transitions |
| **Metric callbacks** | `add_metric_cb()` for custom metric collection |
| **Health check loops** | Periodic health checks (30s) with async/sync callback support |
| **tcsctl CLI** | Terminal UI showing service tree, heartbeat status, uptime, restart info |
| **Graceful degradation** | `DummyMonitoredObject` when NATS unavailable |
| **ServiceControlClient** | Programmatic discovery via JetStream history queries |

**TCS Status Enum** (11 states): UNKNOWN → STARTUP → OK → IDLE → BUSY → DEGRADED → WARNING → ERROR → SHUTDOWN → FAILED

**NATS Streams:**
- `svc_registry` — lifecycle events (indefinite retention)
- `svc_status` — status updates (30-day retention)
- `svc_heartbeat` — heartbeats (1-day retention)

### 3.3 PMS OpenTelemetry (Experimental)

PMS's `copilot/add-nats-aggregator-module` branch introduces:

```python
# pms/telemetry.py
TracerProvider with optional OTLP export
Rotating file handler (10MB × 5)
Per-module tracer instances

# pms/aggregator.py — trace spans:
- aggregator_start (total startup time)
- subscribe_all (subscription hydration)
- collect_{output} (individual collection cycles)
```

**Current state:** OpenTelemetry API/SDK in PMS `pyproject.toml`, used only in the aggregator module. Not integrated with serverish or TCS. No shared telemetry configuration.

---

## 4. The Observability Gap

```
┌─────────────────────────────────────────────────────────┐
│                   External Backends                      │
│  Grafana  │  Jaeger  │  Prometheus  │  Loki  │  OTLP   │
└──────────────────────┬──────────────────────────────────┘
                       │  ← MISSING BRIDGE
┌──────────────────────┴──────────────────────────────────┐
│                    TCS Monitoring                         │
│  Heartbeats │ Status │ Registry │ tcsctl │ Metrics      │
│  (NATS-based, observatory-internal)                      │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────────┐
│                 Serverish Foundation                      │
│  health_status │ diagnostics │ tracing │ journal │ ...  │
│  (in-process, per-component)                             │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────────┐
│                   NATS JetStream                         │
│              (transport layer)                            │
└─────────────────────────────────────────────────────────┘
```

**Three gaps:**

1. **No bridge from NATS monitoring → OpenTelemetry** — TCS's rich monitoring data stays locked in NATS; external tools can't see it
2. **No distributed tracing across services** — When a message flows ocabox-server → PMS → halina, there's no correlated trace
3. **No standardized metrics export** — Each component tracks counters internally but doesn't expose them as metrics

---

## 5. What Can Be Cannibalized from TCS

TCS's `monitoring/` module (795 LOC) is **largely observatory-agnostic**:

### Universal (move to serverish)

| Component | LOC | Why Universal |
|-----------|-----|---------------|
| `Status` enum | 130 | Generic service states (OK, BUSY, ERROR, etc.) — no astronomy deps |
| `MonitoredObject` base | ~300 | Hierarchical status aggregation, task tracking, health check loops — pure patterns |
| `create_monitor()` factory | 74 | Auto-selects NATS vs dummy implementation — useful for any serverish app |
| Heartbeat protocol | ~100 | Periodic signals over NATS — universal pattern |

### Observatory-Specific (stays in TCS)

| Component | Why Specific |
|-----------|-------------|
| `ServiceController` | TCS launcher integration, ProcessContext coupling |
| `tcsctl` CLI | TCS-specific display and service model |
| `ServiceControlClient` | TCS registry/lifecycle event format |
| Launcher monitoring | TCS process management |

### Proposed Split

```
serverish (new: serverish.monitoring)
├── status.py          ← from TCS, generalized
├── monitored.py       ← MonitoredObject base + MessengerMonitoredObject
├── heartbeat.py       ← heartbeat protocol
├── health.py          ← health check loop + metric callbacks
└── __init__.py        ← create_monitor() factory

ocabox-tcs (simplified)
├── monitoring/
│   ├── tcs_monitor.py ← TCS-specific extensions (service controller integration)
│   └── ...
├── tcsctl/            ← stays, uses serverish.monitoring as base
└── ...
```

**Benefit:** Every serverish-based project gets monitoring for free, not just TCS-managed services.

---

## 6. OpenTelemetry Integration Strategy

### 6.1 Approach: Bridge, Don't Replace

The NATS-based monitoring protocol is **the right choice** for:
- Real-time observatory operations (sub-second status)
- Offline/disconnected operation (NATS replays)
- Custom tooling (tcsctl, oca_monitor, TOI)

OpenTelemetry is **the right choice** for:
- Long-term metric storage and alerting
- Distributed tracing across services
- Integration with industry tools (Grafana, Jaeger, PagerDuty)
- Standardized instrumentation libraries

**Strategy: Dual-write with NATS as primary**

```
Service Code
    │
    ├──→ serverish.monitoring (NATS) ──→ tcsctl, oca_monitor, TOI
    │         │
    │         └──→ OTel Bridge ──→ OTLP Collector ──→ Grafana/Jaeger/Loki
    │
    └──→ serverish.tracing (OTel spans) ──→ OTLP Collector
```

### 6.2 Concrete Modules

#### `serverish.telemetry.metrics` — Metric Bridge

```python
# Expose serverish health_status as OTel metrics
from serverish.telemetry.metrics import instrument_publisher, instrument_reader

pub = get_publisher("telemetry.weather")
instrument_publisher(pub)  # Creates OTel gauges/counters from health_status

# Automatic metrics:
# serverish.publisher.count{subject="telemetry.weather"}
# serverish.publisher.errors{subject="telemetry.weather"}
# serverish.reader.pending{subject="telemetry.weather"}
# serverish.connection.slow_consumers{}
```

#### `serverish.telemetry.tracing` — Distributed Tracing

```python
# Inject trace context into NATS message metadata
from serverish.telemetry.tracing import traced_publish, traced_read

# Publisher side: adds trace_id to meta
await traced_publish(pub, data)

# Reader side: extracts trace_id, creates child span
async for data, meta in traced_read(reader):
    # meta['otel_trace_id'] links to parent span
    process(data)
```

**Propagation:** Add `otel_context` to message `meta` dict (backward-compatible — old readers ignore it).

#### `serverish.telemetry.logging` — Log Bridge

```python
# Bridge journal publisher to OTel logging
from serverish.telemetry.logging import OTelJournalHandler

# Existing NatsJournalLoggingHandler sends to NATS
# OTelJournalHandler additionally sends to OTLP logs endpoint
```

### 6.3 Dependency Strategy

OpenTelemetry should be an **optional extra**, like messenger:

```toml
[tool.poetry.extras]
messenger = ["nats-py"]
telemetry = ["opentelemetry-api", "opentelemetry-sdk"]
full = ["nats-py", "opentelemetry-api", "opentelemetry-sdk"]
```

Zero-cost when not installed — all OTel integration is behind import guards.

---

## 7. Parallel Milestones Assessment

The user asks whether the **observability milestone** and the **NATS reader redesign** (per `nats-client-considerations.md`) can run in parallel.

### Can They Be Parallel?

**Yes, with one dependency to manage.**

| Aspect | Observability Milestone | Reader Redesign Milestone |
|--------|------------------------|--------------------------|
| **Core files** | New modules: `serverish/monitoring/`, `serverish/telemetry/` | Existing: `serverish/messenger/msg_reader.py`, new thread architecture |
| **Touches reader?** | Adds metric instrumentation wrapper (non-invasive) | Rewrites reader internals (thread separation, queue architecture) |
| **Dependency** | Metric bridge reads `health_status` — needs stable API | `health_status` API is already stable |
| **Risk** | Low — additive modules | High — core transport change |

**Recommended approach:**

1. **Reader redesign first** (or in parallel with monitoring cannibalization from TCS)
   - This changes the internal architecture of `msg_reader.py`
   - The `health_status` property API should remain stable (it's a dict)
   - New metrics (e.g., cross-thread queue depth) become available

2. **OTel bridge second** (after reader is stable)
   - Instrument the new reader architecture
   - Distributed tracing needs the new thread model to work correctly (trace context propagation across threads)

3. **Monitoring module from TCS** can happen in parallel with both
   - It's additive (new `serverish/monitoring/` package)
   - Doesn't touch existing messenger code
   - Can be extracted and adapted independently

```
Timeline:
  ┌─ Monitoring (from TCS) ──────────────────────┐
  │  Extract → Generalize → Test → Integrate      │
  ├────────────────────────────────────────────────┤
  │                                                │
  ├─ Reader Redesign ─────────────────┐            │
  │  Thread separation → Queue arch   │            │
  │  → ACK strategies → Reconnect     │            │
  ├───────────────────────────────────┤            │
  │                                   │            │
  │              ┌─ OTel Bridge ──────┴────────────┤
  │              │  Metrics → Tracing → Logging    │
  │              └─────────────────────────────────┤
  └────────────────────────────────────────────────┘
```

---

## 8. Suggested Roadmap

### Phase A: Monitoring Foundation (from TCS)
**Goal:** Every serverish-based service gets basic monitoring without TCS dependency.

- Extract `Status` enum, `MonitoredObject`, heartbeat protocol from ocabox-tcs
- Generalize (remove TCS-specific coupling)
- Add to serverish as `serverish.monitoring`
- Provide `create_monitor()` factory with NATS/dummy auto-selection
- Update TCS to import from serverish instead of its own copy

### Phase B: NATS Reader Redesign
**Goal:** Reliable message delivery under adverse conditions (per `nats-client-considerations.md`).

- Thread-separated NATS loop (transport isolation)
- Queue architecture with explicit ACK strategies
- Consumer scaling (fan-in)
- Reconnect with pull-loop restart
- Enhanced `health_status` with queue depth, thread health

### Phase C: OpenTelemetry Bridge
**Goal:** Industry-standard observability for any serverish service.

- `serverish.telemetry.metrics` — auto-instrument publishers, readers, connections
- `serverish.telemetry.tracing` — distributed trace context in NATS messages
- `serverish.telemetry.logging` — bridge journal to OTel logs
- Optional dependency (`telemetry` extra)
- Zero-cost when not installed

### Phase D: Unified Observability
**Goal:** Single configuration point for all observability in a serverish service.

- `Messenger(telemetry=True)` enables all bridges
- Auto-discovers OTLP endpoint
- NATS monitoring + OTel export from single config
- Dashboard templates for Grafana (serverish service template)

---

## 9. Key Design Principles

1. **NATS is primary, OTel is bridge** — Observatory operations depend on NATS real-time data; OTel is for external integration and long-term storage
2. **Optional everything** — No service should break because OTel isn't configured
3. **Zero-cost abstraction** — If telemetry extra isn't installed, no import overhead
4. **Backward compatible** — New meta fields (otel_context) are ignored by old readers
5. **Cannibalize wisely** — Only move truly universal patterns from TCS to serverish; keep domain-specific logic in TCS
6. **health_status as contract** — The dict-based health_status property is the stable API that both monitoring and OTel bridge depend on

---

## 10. References

- `serverish/doc/nats-client-considerations.md` — Reader redesign architecture (thread separation, ACK strategies)
- `ocabox-tcs/doc/nats.md` — TCS NATS messaging schema
- `ocabox-tcs/doc/feature-roadmap.md` — TCS planned features
- `pms/PMS_TCS_MIGRATION.md` — PMS migration to TCS framework
- `pms/telemetry.py` — PMS OpenTelemetry experimental integration
- `obsidian/Projects/Core Services/serverish.md` — Serverish in observatory context
- `obsidian/Architecture/System Overview.md` — OCM system architecture
