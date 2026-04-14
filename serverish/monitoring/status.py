"""Status enums and utilities for service monitoring.

Provides service status tracking with hierarchical aggregation.
Status levels follow severity precedence: FAILED > ERROR > WARNING >
DEGRADED > STARTUP > SHUTDOWN > BUSY > IDLE/OK > UNKNOWN.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from serverish.base import dt_from_array, dt_utcnow_array


class Status(Enum):
    """Service/component status levels.

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
        """Check if status indicates healthy state."""
        return self in (Status.OK, Status.IDLE, Status.BUSY, Status.DEGRADED, Status.WARNING)

    @property
    def is_operational(self) -> bool:
        """Check if status indicates service is operational."""
        return self in (Status.STARTUP, Status.OK, Status.IDLE, Status.BUSY, Status.DEGRADED, Status.WARNING)


@dataclass
class StatusReport:
    """Status report for a monitored component.

    Args:
        name: Component name
        status: Current status
        message: Optional human-readable message
        timestamp: UTC timestamp as 7-element array [Y, M, D, h, m, s, us]
        details: Optional dict with children reports and metrics
        parent: Optional parent name for hierarchical grouping
    """
    name: str
    status: Status
    message: str | None = None
    timestamp: list[int] | None = None
    details: dict[str, Any] | None = None
    parent: str | None = None

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
        )


def aggregate_status(reports: list[StatusReport]) -> Status:
    """Aggregate multiple status reports into single status.

    Follows severity precedence:
    FAILED > ERROR > WARNING > DEGRADED > STARTUP > SHUTDOWN > BUSY > IDLE/OK > UNKNOWN
    """
    if not reports:
        return Status.UNKNOWN

    statuses = [report.status for report in reports]

    if Status.FAILED in statuses:
        return Status.FAILED
    if Status.ERROR in statuses:
        return Status.ERROR
    if Status.WARNING in statuses:
        return Status.WARNING
    if Status.DEGRADED in statuses:
        return Status.DEGRADED
    if Status.STARTUP in statuses:
        return Status.STARTUP
    if Status.SHUTDOWN in statuses:
        return Status.SHUTDOWN
    if Status.BUSY in statuses:
        return Status.BUSY

    if all(s in (Status.IDLE, Status.OK) for s in statuses):
        if Status.IDLE in statuses:
            return Status.IDLE
        return Status.OK

    return Status.WARNING
