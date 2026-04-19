"""MonitoredObject hierarchy for status reporting and health checking.

Provides base classes for monitored entities with:
- Parent-child status aggregation
- Task tracking (BUSY/IDLE transitions)
- Health check and metric callbacks
- Periodic heartbeat and health check loops
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from serverish.base.status import Status, StatusReport, aggregate_status


class MonitoredObject:
    """Base class for monitored objects with aggregation support.

    Provides:
    - Hierarchical parent-child monitoring
    - Status tracking with change notifications
    - Task tracking for automatic BUSY/IDLE transitions
    - Health check and metric callbacks
    - Full status reports with children and metrics

    Args:
        name: Unique name for this monitored object
        parent: Optional parent MonitoredObject for hierarchy
    """

    def __init__(self, name: str, parent: MonitoredObject | None = None):
        self.name = name
        self.parent = parent
        self.children: dict[str, MonitoredObject] = {}
        self._status = Status.UNKNOWN
        self._message: str | None = None
        self._healthcheck_callbacks: list[Callable[[], Status | None]] = []
        self._metric_callbacks: list[Callable[[], dict[str, Any]]] = []
        self.logger = logging.getLogger(f"mon|{name}")

        self._active_tasks = 0
        self._idle_transition_task: asyncio.Task | None = None
        self._task_tracking_enabled = False

        if parent:
            parent.add_submonitor(self)

    async def __aenter__(self):
        """Enter context: start monitoring."""
        await self.start_monitoring()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit context: stop monitoring."""
        await self.stop_monitoring()
        return False

    async def start_monitoring(self):
        """Start monitoring. Override in subclasses."""

    async def stop_monitoring(self):
        """Stop monitoring. Override in subclasses."""

    async def send_registration(self):
        """Send registration event. Override in subclasses that support it."""

    async def send_shutdown(self):
        """Send shutdown event. Override in subclasses that support it."""

    def track_task(self, name: str | None = None) -> _TaskTracker:
        """Context manager for tracking task execution (enables BUSY/IDLE status).

        Args:
            name: Optional name for the task (for logging)

        Usage::

            async with monitor.track_task('processing'):
                await do_work()

        On first use, enables task tracking and switches from OK to BUSY/IDLE.
        Task start: immediately BUSY. Task end: 1s delay then IDLE.
        If another task starts during delay, stays BUSY.
        """
        return _TaskTracker(self)

    async def _task_started(self):
        """Internal: Called when a task starts."""
        self._active_tasks += 1

        if not self._task_tracking_enabled:
            self._task_tracking_enabled = True
            self.logger.debug("Task tracking enabled")

        if self._idle_transition_task and not self._idle_transition_task.done():
            self._idle_transition_task.cancel()
            try:
                await self._idle_transition_task
            except asyncio.CancelledError:
                pass
            self._idle_transition_task = None

        if self._status not in (Status.BUSY, Status.ERROR, Status.FAILED):
            self.set_status(Status.BUSY, f"Processing tasks ({self._active_tasks} active)")

    async def _task_finished(self):
        """Internal: Called when a task finishes."""
        self._active_tasks -= 1

        if self._active_tasks < 0:
            self.logger.warning("Task counter went negative, resetting to 0")
            self._active_tasks = 0

        if self._active_tasks == 0:
            self._idle_transition_task = asyncio.create_task(self._delayed_idle_transition())
        else:
            self.set_status(Status.BUSY, f"Processing tasks ({self._active_tasks} active)")

    async def _delayed_idle_transition(self):
        """Internal: Wait 1s then transition to IDLE if no new tasks."""
        try:
            await asyncio.sleep(1.0)
            if self._active_tasks == 0 and self._status == Status.BUSY:
                self.set_status(Status.IDLE, "No active tasks")
        except asyncio.CancelledError:
            pass

    def cancel_error_status(self):
        """If currently in ERROR/FAILED/DEGRADED status, revert to OK/IDLE/BUSY."""
        if self._status in (Status.ERROR, Status.FAILED, Status.DEGRADED):
            if self._task_tracking_enabled:
                new_status = Status.IDLE if self._active_tasks == 0 else Status.BUSY
            else:
                new_status = Status.OK
            self.set_status(new_status, "Error resolved")

    def set_status(self, status: Status, message: str | None = None):
        """Set status directly and trigger status change notification."""
        old_status = self._status
        self._status = status
        self._message = message
        self.logger.debug(f"Status set to {status}: {message or ''}")

        if old_status != status:
            self._on_status_changed()

    def _on_status_changed(self):
        """Called when status changes. Override in subclasses to send updates."""

    def add_healthcheck_cb(self, callback: Callable[[], Status | None]):
        """Add healthcheck callback (sync or async).

        Callbacks are called periodically during healthcheck loop (default 30s).
        Return None for healthy, or a Status for unhealthy state.
        """
        self._healthcheck_callbacks.append(callback)

    def add_metric_cb(self, callback: Callable[[], dict[str, Any]]):
        """Add metric callback (sync or async).

        Called when generating status reports. Returned dict is included
        in the report's details.metrics section.
        """
        self._metric_callbacks.append(callback)

    def add_submonitor(self, child: MonitoredObject):
        """Add child monitor."""
        self.children[child.name] = child
        child.parent = self
        self.logger.debug(f"Added submonitor: {child.name}")

    def remove_submonitor(self, name: str):
        """Remove child monitor."""
        if name in self.children:
            self.children[name].parent = None
            del self.children[name]
            self.logger.debug(f"Removed submonitor: {name}")

    def get_status(self) -> Status:
        """Get current status."""
        return self._status

    async def healthcheck(self) -> Status:
        """Perform health check using callbacks (supports sync and async).

        Returns first unhealthy status from callbacks, or current status
        if all callbacks return healthy.
        """
        for callback in self._healthcheck_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    status = await callback()
                else:
                    status = callback()

                if status is not None and not status.is_healthy:
                    return status
            except Exception as e:
                self.logger.warning(f"Healthcheck callback failed: {e}")
                return Status.ERROR

        return self.get_status()

    async def get_full_report(self) -> StatusReport:
        """Get complete status report including children and metrics."""
        own_status = self.get_status()

        metrics: dict[str, Any] = {}
        for callback in self._metric_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    metric_data = await callback()
                else:
                    metric_data = callback()

                if metric_data and isinstance(metric_data, dict):
                    metrics.update(metric_data)
            except Exception as e:
                self.logger.warning(f"Metric callback failed: {e}")

        child_reports = []
        for child in self.children.values():
            child_reports.append(await child.get_full_report())

        if child_reports:
            child_statuses = [report.status for report in child_reports]
            aggregated = aggregate_status([
                StatusReport(self.name, own_status),
                *[StatusReport(f"child_{i}", status) for i, status in enumerate(child_statuses)]
            ])
        else:
            aggregated = own_status

        details = None
        if child_reports or metrics:
            details = {}
            if child_reports:
                details["own_status"] = own_status.value
                details["children"] = [report.to_dict() for report in child_reports]
            if metrics:
                details["metrics"] = metrics

        return StatusReport(
            name=self.name,
            status=aggregated,
            message=self._message,
            details=details,
        )


class _TaskTracker:
    """Context manager for tracking task execution in MonitoredObject.

    Supports both async and sync context managers::

        # Async (preferred)
        async with monitor.track_task('work'):
            await do_async_work()

        # Sync (for non-async code — schedules coroutines on running loop)
        with monitor.track_task('work'):
            do_sync_work()
    """

    def __init__(self, monitor: MonitoredObject):
        self.monitor = monitor

    async def __aenter__(self):
        await self.monitor._task_started()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.monitor._task_finished()
        return False

    def __enter__(self):
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.monitor._task_started())
        except RuntimeError:
            pass
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.monitor._task_finished())
        except RuntimeError:
            pass
        return False


class ReportingMonitoredObject(MonitoredObject):
    """MonitoredObject that actively sends heartbeats and performs health checks.

    Adds periodic loops on top of MonitoredObject:
    - Heartbeat loop (default every 10s)
    - Health check loop (default every 30s)

    Args:
        name: Unique name for this monitored object
        parent: Optional parent MonitoredObject
        check_interval: Heartbeat interval in seconds (default: 10.0)
        healthcheck_interval: Health check interval in seconds (default: 30.0)
    """

    def __init__(
        self,
        name: str,
        parent: MonitoredObject | None = None,
        check_interval: float = 10.0,
        healthcheck_interval: float = 30.0,
    ):
        super().__init__(name, parent)
        self.check_interval = check_interval
        self.healthcheck_interval = healthcheck_interval
        self._heartbeat_task: asyncio.Task | None = None
        self._healthcheck_task: asyncio.Task | None = None
        self._running = False

    async def start_monitoring(self):
        """Start periodic monitoring (heartbeat and healthcheck loops)."""
        if self._running:
            return

        self._running = True
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._healthcheck_task = asyncio.create_task(self._healthcheck_loop())

        self.logger.info(
            f"Started monitoring: heartbeat={self.check_interval}s, "
            f"healthcheck={self.healthcheck_interval}s"
        )

    async def stop_monitoring(self):
        """Stop periodic monitoring (both loops)."""
        self._running = False

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        if self._healthcheck_task:
            self._healthcheck_task.cancel()
            try:
                await self._healthcheck_task
            except asyncio.CancelledError:
                pass
            self._healthcheck_task = None

        self.logger.info("Stopped monitoring")

    async def _heartbeat_loop(self):
        """Heartbeat loop — sends periodic heartbeats."""
        while self._running:
            try:
                await self._send_heartbeat()
                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Heartbeat loop error: {e}")
                await asyncio.sleep(min(self.check_interval, 10.0))

    async def _healthcheck_loop(self):
        """Healthcheck loop — performs periodic health checks and updates status."""
        while self._running:
            try:
                status = await self.healthcheck()
                if status != self.get_status():
                    self.set_status(status, "Updated from healthcheck")
                await asyncio.sleep(self.healthcheck_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Healthcheck loop error: {e}")
                await asyncio.sleep(min(self.healthcheck_interval, 10.0))

    async def _send_heartbeat(self):
        """Send heartbeat message. Override in subclasses for NATS publishing."""
        self.logger.debug(f"Heartbeat from {self.name}")

    async def _send_status_report(self):
        """Send status report. Override in subclasses for NATS publishing."""
        report = await self.get_full_report()
        self.logger.debug(f"Status changed to {report.status} - {report.message or 'OK'}")


class DummyMonitoredObject(MonitoredObject):
    """No-op monitored object for graceful degradation without NATS.

    Provides the same API as other implementations but does nothing.
    Useful for development, testing, or environments where monitoring is disabled.
    """

    async def start_monitoring(self):
        self.logger.debug(f"DummyMonitor: start_monitoring() called for {self.name}")

    async def stop_monitoring(self):
        self.logger.debug(f"DummyMonitor: stop_monitoring() called for {self.name}")

    def _on_status_changed(self):
        self.logger.debug(f"DummyMonitor: status changed to {self._status}")
