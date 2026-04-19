"""Unified status types for serverish.

Provides a single Status enum and StatusReport dataclass used by every layer
of the library: diagnostics (HasStatuses), connections, monitoring, and tasks.

Status severity precedence (highest wins in aggregation):
    FAILED > ERROR > WARNING > DEGRADED > STARTUP > SHUTDOWN > BUSY > IDLE > OK > UNKNOWN

This module replaces the former dual-system design where serverish.base had a
4-value StatusEnum and serverish.monitoring had a separate 10-value Status enum.
The two are now unified here.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from serverish.base.datetime import dt_from_array, dt_utcnow_array


class Status(Enum):
    """Component/service status.

    Ordered by severity (highest wins in aggregation):
    FAILED > ERROR > WARNING > DEGRADED > STARTUP > SHUTDOWN > BUSY > IDLE > OK > UNKNOWN

    Properties:
        is_healthy: True for OK, IDLE, BUSY, DEGRADED, WARNING
        is_operational: True for STARTUP, OK, IDLE, BUSY, DEGRADED, WARNING
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

    def __str__(self) -> str:
        return self.value

    @property
    def is_healthy(self) -> bool:
        """True for OK, IDLE, BUSY, DEGRADED, WARNING."""
        return self in _HEALTHY_STATUSES

    @property
    def is_operational(self) -> bool:
        """True for STARTUP, OK, IDLE, BUSY, DEGRADED, WARNING."""
        return self in _OPERATIONAL_STATUSES


_HEALTHY_STATUSES = frozenset({
    Status.OK, Status.IDLE, Status.BUSY, Status.DEGRADED, Status.WARNING,
})

_OPERATIONAL_STATUSES = frozenset({
    Status.STARTUP, Status.OK, Status.IDLE, Status.BUSY,
    Status.DEGRADED, Status.WARNING,
})

_SEVERITY_RANK: dict[Status, int] = {
    Status.UNKNOWN: 0,
    Status.OK: 1,
    Status.IDLE: 2,
    Status.BUSY: 3,
    Status.SHUTDOWN: 4,
    Status.STARTUP: 5,
    Status.DEGRADED: 6,
    Status.WARNING: 7,
    Status.ERROR: 8,
    Status.FAILED: 9,
}


@dataclass
class StatusReport:
    """Status report for any component.

    Used both as a diagnostic check result (with deduce_other for deduction
    chains in HasStatuses) and as a rich status report (with details, children,
    and metrics for monitoring).

    Args:
        name: Component name
        status: Current status
        message: Optional human-readable message
        timestamp: UTC timestamp as 7-element array [Y, M, D, h, m, s, us]
        details: Optional dict with children reports and metrics
        parent: Optional parent name for hierarchical grouping
        deduce_other: If True, subsequent diagnostic checks are deduced from this result
    """
    name: str
    status: Status
    message: str | None = None
    timestamp: list[int] | None = None
    details: dict[str, Any] | None = None
    parent: str | None = None
    deduce_other: bool = False

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = dt_utcnow_array()

    def get_timestamp_dt(self):
        """Get timestamp as datetime object."""
        if self.timestamp is None:
            return None
        return dt_from_array(self.timestamp)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        result: dict[str, Any] = {
            "name": self.name,
            "status": self.status.value,
            "timestamp": self.timestamp,
        }
        if self.message:
            result["message"] = self.message
        if self.details:
            result["details"] = self.details
        if self.parent:
            result["parent"] = self.parent
        if self.deduce_other:
            result["deduce_other"] = True
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StatusReport:
        """Create from dictionary.

        Raises:
            KeyError: If required fields 'name' or 'status' are missing
            ValueError: If 'status' value is not a valid Status enum member
        """
        if "name" not in data or "status" not in data:
            missing = [k for k in ("name", "status") if k not in data]
            raise KeyError(f"StatusReport.from_dict missing required fields: {missing}")
        return cls(
            name=data["name"],
            status=Status(data["status"]),
            message=data.get("message"),
            timestamp=data.get("timestamp"),
            details=data.get("details"),
            parent=data.get("parent"),
            deduce_other=data.get("deduce_other", False),
        )

    @staticmethod
    def ok(msg: str | None = None, deduce_other: bool = True) -> StatusReport:
        """Create an OK status report. Defaults deduce_other=True."""
        return StatusReport(name="", status=Status.OK, message=msg, deduce_other=deduce_other)

    @staticmethod
    def error(msg: str | None = None, deduce_other: bool = False) -> StatusReport:
        """Create an ERROR status report."""
        return StatusReport(name="", status=Status.ERROR, message=msg, deduce_other=deduce_other)

    @staticmethod
    def failed(msg: str | None = None, deduce_other: bool = False) -> StatusReport:
        """Create a FAILED status report."""
        return StatusReport(name="", status=Status.FAILED, message=msg, deduce_other=deduce_other)

    @staticmethod
    def unknown(msg: str | None = None, deduce_other: bool = False) -> StatusReport:
        """Create an UNKNOWN status report."""
        return StatusReport(name="", status=Status.UNKNOWN, message=msg, deduce_other=deduce_other)

    @staticmethod
    def shutdown(msg: str | None = None, deduce_other: bool = False) -> StatusReport:
        """Create a SHUTDOWN status report."""
        return StatusReport(name="", status=Status.SHUTDOWN, message=msg, deduce_other=deduce_other)

    @staticmethod
    def degraded(msg: str | None = None, deduce_other: bool = False) -> StatusReport:
        """Create a DEGRADED status report."""
        return StatusReport(name="", status=Status.DEGRADED, message=msg, deduce_other=deduce_other)

    @staticmethod
    def warning(msg: str | None = None, deduce_other: bool = False) -> StatusReport:
        """Create a WARNING status report."""
        return StatusReport(name="", status=Status.WARNING, message=msg, deduce_other=deduce_other)

    @classmethod
    def deduced(cls, source: StatusReport, msg: str | None = None) -> StatusReport:
        """Create a status report deduced from another result.

        Used by HasStatuses diagnostic chains when a prior check's
        deduce_other=True allows skipping subsequent checks.

        Args:
            source: The StatusReport to deduce from
            msg: Optional message (defaults to 'Deduced')
        """
        if msg is None:
            msg = "Deduced"
        return cls(
            name="",
            status=source.status,
            message=msg,
            deduce_other=False,
        )


def aggregate_status(reports: list[StatusReport]) -> Status:
    """Aggregate multiple status reports into single status.

    Returns the most severe status from the collection, following
    severity precedence:
    FAILED > ERROR > WARNING > DEGRADED > STARTUP > SHUTDOWN > BUSY > IDLE > OK > UNKNOWN

    Args:
        reports: List of StatusReport instances

    Returns:
        The most severe Status from the reports, or Status.UNKNOWN if empty
    """
    if not reports:
        return Status.UNKNOWN
    return max((r.status for r in reports), key=lambda s: _SEVERITY_RANK[s])
