# Status System Unification Analysis

## The Question

Serverish currently has two parallel status systems:

1. **`serverish.base.status`** — `StatusEnum` (4 values: ok/fail/na/disabled) + `Status` dataclass (wrapping the enum with a message and deduction flag)
2. **`serverish.monitoring.status`** — `Status` enum (10 values: UNKNOWN through FAILED) + `StatusReport` dataclass

A `bridge.py` module translates between them. Should serverish unify these into a single system?

**Yes.** The rest of this document explains why, what the unified design looks like, and what breaks.

---

## Why Unify

The two systems exist because of history, not because of a genuine domain distinction.

`StatusEnum` was written when serverish only needed diagnostic checks: "is the connection alive?" Its vocabulary — `ok`, `fail`, `na`, `disabled` — is the vocabulary of a single boolean probe. It was never designed to express richer states like "operational but degraded" or "busy processing."

`monitoring.Status` was cannibalized from TCS, where services need a full lifecycle vocabulary. It naturally subsumes everything `StatusEnum` can express:

| StatusEnum | monitoring.Status equivalent |
|---|---|
| `ok` | `OK` (or `IDLE`, `BUSY` depending on context) |
| `fail` | `ERROR` (or `FAILED` for permanent) |
| `na` | `UNKNOWN` |
| `disabled` | `SHUTDOWN` |

The bridge is a symptom, not a feature. Every time someone reads the codebase, they must understand two enum types, a dataclass, a separate report dataclass, a translation function, the subtle differences in semantics, and why `Status` means different things depending on which import you used. This is precisely the kind of compatibility layer that accumulates into technical debt.

The monitoring system also has properties the diagnostic system should have had from the start: `is_healthy`, `is_operational`, severity-based aggregation. These are universally useful, not monitoring-specific.

---

## The Unified Design

### One Enum: `serverish.base.Status`

```python
class Status(Enum):
    """Component/service status.

    Ordered by severity (highest wins in aggregation):
    FAILED > ERROR > WARNING > DEGRADED > STARTUP > SHUTDOWN > BUSY > IDLE > OK > UNKNOWN
    """
    UNKNOWN = "unknown"
    STARTUP = "startup"
    OK = "ok"
    IDLE = "idle"
    BUSY = "busy"
    DEGRADED = "degraded"
    WARNING = "warning"
    ERROR = "error"
    SHUTDOWN = "shutdown"
    FAILED = "failed"

    @property
    def is_healthy(self) -> bool:
        return self in (Status.OK, Status.IDLE, Status.BUSY, Status.DEGRADED, Status.WARNING)

    @property
    def is_operational(self) -> bool:
        return self in (Status.STARTUP, Status.OK, Status.IDLE, Status.BUSY, Status.DEGRADED, Status.WARNING)
```

This is the current `monitoring.Status` moved to `serverish.base.status` and becoming **the** status enum for the entire library. It lives at the foundation layer because everything — connections, tasks, monitors — uses it.

### One Report: `serverish.base.StatusReport`

```python
@dataclass
class StatusReport:
    """Status report for any component."""
    name: str
    status: Status
    message: str | None = None
    timestamp: list[int] | None = None
    details: dict[str, Any] | None = None
    parent: str | None = None
    deduce_other: bool = False
```

This merges the old `Status` dataclass (which was really a "check result" not a "status") with `StatusReport`. The key insight: the old `Status` dataclass had three fields — `status`, `msg`, `deduce_other`. That's a diagnostic check result. `StatusReport` is a richer version of the same idea. Adding `deduce_other: bool = False` to `StatusReport` absorbs the deduction capability without a separate type.

The factory methods (`new_ok`, `new_fail`, etc.) become static methods on `StatusReport`:

```python
    @staticmethod
    def ok(msg: str | None = None, deduce_other: bool = True) -> StatusReport:
        return StatusReport(name="", status=Status.OK, message=msg, deduce_other=deduce_other)

    @staticmethod
    def error(msg: str | None = None) -> StatusReport:
        return StatusReport(name="", status=Status.ERROR, message=msg)

    @staticmethod
    def unknown(msg: str | None = None) -> StatusReport:
        return StatusReport(name="", status=Status.UNKNOWN, message=msg)

    @staticmethod
    def shutdown(msg: str | None = None) -> StatusReport:
        return StatusReport(name="", status=Status.SHUTDOWN, message=msg)
```

Note: the old `new_fail` becomes `error()` (or `failed()` for permanent), `new_na` becomes `unknown()`, `new_disabled` becomes `shutdown()`. This is a vocabulary improvement, not just a rename — the old names leaked implementation details ("na" is not a user-facing concept; "unknown" is).

### `HasStatuses` stays, uses new types

`HasStatuses` is a good pattern — ordered diagnostic checks with deduction is genuinely useful. It just needs to speak the new vocabulary:

```python
class HasStatuses(Manageable):
    async def diagnose(self, no_deduce: bool = False) -> dict[str, StatusReport]:
        # Same logic, same deduction, same ordered checks
        # Returns StatusReport instead of old Status dataclass
        ...

    def point_of_failure(self) -> tuple[str | None, StatusReport | None]:
        for k, s in reversed(self.status.items()):
            if s.status == Status.ERROR or s.status == Status.FAILED:
                return k, s
        return None, None
```

The check methods signature changes from `() -> Status` (the old dataclass) to `() -> StatusReport`. Since `StatusReport` has `deduce_other`, the deduction logic works identically.

### `aggregate_status` moves to `serverish.base`

Status aggregation (worst-status-wins) is a base-layer concept, not monitoring-specific. Connections with multiple checks need it. Task managers with multiple tasks need it. Moving it to `serverish.base.status` alongside the enum is the right home.

### The bridge dies

`serverish/monitoring/bridge.py` translation functions become unnecessary. The diagnostic system and monitoring system speak the same language. `bind_diagnostics` still has value (connecting `HasStatuses.diagnose()` results to a `MonitoredObject` healthcheck callback), but it no longer translates between enum types — it just passes through.

### `health_status` property

The `health_status` dict on `ConnectionNATS`, publishers, readers is orthogonal to this unification. It's an instrumentation dict, not a status enum. It stays as-is. The monitoring module can still wrap it via metric callbacks.

---

## What Breaks

### Inside serverish (all fixable in one pass)

| File | Change |
|---|---|
| `serverish/base/status.py` | Replace `StatusEnum` + old `Status` dataclass with new `Status` enum + `StatusReport` |
| `serverish/base/__init__.py` | Export new types, add backward-compat aliases if wanted (but per project philosophy: don't) |
| `serverish/base/hasstatuses.py` | `diagnose()` returns `dict[str, StatusReport]`; `point_of_failure` checks `Status.ERROR`/`Status.FAILED` instead of `StatusEnum.fail` |
| `serverish/base/task_manager.py` | `Status.new_ok()` → `StatusReport.ok()`, etc. |
| `serverish/connection/connection.py` | Same: factory method renames |
| `serverish/connection/connection_nats.py` | Same |
| `serverish/connection/connection_jets.py` | Same |
| `serverish/monitoring/status.py` | **Deleted** — `Status` and `StatusReport` now live in `base` |
| `serverish/monitoring/bridge.py` | Translation functions deleted; `bind_diagnostics` simplified to pass-through |
| `serverish/monitoring/monitored.py` | Imports from `serverish.base` instead of `serverish.monitoring.status` |
| `serverish/monitoring/monitored_nats.py` | Same import change |
| `serverish/monitoring/factory.py` | Same import change |
| `serverish/monitoring/__init__.py` | Re-exports from `serverish.base` |
| All tests | `StatusEnum.ok` → `Status.OK`, `Status.new_fail(msg=...)` → `StatusReport.error(msg=...)` |

### Outside serverish (downstream projects)

From the ecosystem scan, direct `StatusEnum` usage is limited:

| Project | Usage | Fix |
|---|---|
| `oca_nats_config` / `camk_nats_config` | `from serverish.base import StatusEnum`; checks `status != StatusEnum.ok` after `diagnose()` | Change to `status.status != Status.OK` or `not status.status.is_healthy` |
| `ocabox-server` | `from serverish.base import StatusEnum` in one test file | Same pattern |
| `ocabox-tcs` | Has its own `monitoring.Status` that mirrors `serverish.monitoring.Status` | Switch to importing from `serverish.monitoring` (which re-exports from `serverish.base`) |

**No other project in the ecosystem imports `StatusEnum` or `base.Status` directly.** The blast radius is small and well-defined.

---

## What the monitoring module becomes after unification

`serverish.monitoring` stops being a parallel status universe and becomes what it should be: **a thin service-monitoring layer built on base primitives**.

```
serverish/monitoring/
├── __init__.py          ← re-exports Status, StatusReport from base + own classes
├── monitored.py         ← MonitoredObject, ReportingMonitoredObject, DummyMonitoredObject
├── monitored_nats.py    ← MessengerMonitoredObject (NATS publishing)
├── factory.py           ← create_monitor()
└── bridge.py            ← bind_diagnostics(), health_status_metric_cb() (no translation)
```

**Deleted:** `status.py` (merged into base)

The `monitoring.__init__.py` re-exports `Status`, `StatusReport`, `aggregate_status` from `serverish.base` so that `from serverish.monitoring import Status` still works. This is not a compatibility shim — it's the natural public API for monitoring users who shouldn't need to know about the base layer.

---

## Design Considerations

### Why not keep StatusEnum as a lightweight alias?

Because aliases are lies. If `StatusEnum.ok` and `Status.OK` both exist and mean the same thing, someone will use the wrong one, and the next reader will wonder if they're different. One vocabulary, one import path, no ambiguity.

### Why StatusReport instead of two separate types (CheckResult + StatusReport)?

Because a check result **is** a status report with minimal fields filled in. The deduction flag is the only thing that distinguishes them, and it's just a boolean. A separate `CheckResult` type would recreate the two-type problem we're solving.

### Why put Status in base, not monitoring?

Because `Connection`, `Task`, and `HasStatuses` — all base-layer classes — need it. Dependencies flow downward. Base depends on nothing. Monitoring depends on base. If Status lived in monitoring, base would need to import from monitoring, creating a circular dependency or forcing an awkward split.

### What about the `__bool__` on old Status?

**Decision: No `__bool__` on StatusReport.** All checks must be explicit: `report.status.is_healthy` or `report.status == Status.OK`. This avoids the subtle semantic widening the old `__bool__` would have caused.

### What about `Status.__eq__` comparisons?

**Decision: No custom `__eq__` on StatusReport.** Use explicit `report.status == Status.OK` instead of `report == Status.OK`. This avoids asymmetric equality (`report == Status.OK` would work but `Status.OK == report` would not) and preserves hashability.

---

## Execution Approach

This is a single coherent refactoring — not something that benefits from being spread across many phases. The changes are mechanical but interconnected: you can't have half the codebase on the old types and half on the new.

**Recommended:** One GSD milestone with 2-3 phases:

1. **Unify types** — Replace `StatusEnum` + old `Status` + `monitoring.Status` + `StatusReport` with new `Status` enum + `StatusReport` in `serverish.base`. Update all serverish code and tests. Delete `monitoring/status.py` and bridge translation functions.
2. **Verify** — Run full test suite, check TCS compatibility, update documentation.
3. **Polish** — Review the monitoring module post-unification for any simplifications that become obvious. Update `doc/monitoring.md` and `doc/observability-report.md`.

Version bump: this is a breaking change to the public API (`StatusEnum` removal, `diagnose()` return type change). Warrants a minor version bump at minimum.

---

## Outcome

**Implemented in v1.8.0** on `feat/monitoring-foundation` branch (2026-04-13).

All recommendations from this analysis were followed, with two refinements from research:
- `StatusReport.__bool__` was omitted entirely (not widened to `is_healthy`) — forces explicit checks
- `StatusReport.__eq__` was kept as default dataclass equality — no cross-type comparison magic

Files changed: 15 (see git log for details). 81 tests pass, 0 regressions.
