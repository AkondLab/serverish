"""NATS-based MonitoredObject that publishes status and heartbeats over Messenger.

Publishes to NATS subjects:
- Status updates: ``<prefix>.status.<name>`` (on status change)
- Heartbeats: ``<prefix>.heartbeat.<name>`` (periodic, default 10s)

Registry/lifecycle events (start, stop, crashed) are the responsibility
of service runners/launchers, not the monitored object itself.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket

from serverish.base import dt_utcnow_array
from serverish.messenger import get_publisher
from serverish.monitoring.monitored import MonitoredObject, ReportingMonitoredObject

logger = logging.getLogger(__name__)


class MessengerMonitoredObject(ReportingMonitoredObject):
    """MonitoredObject that sends reports to NATS via serverish Messenger.

    Args:
        name: Service name (e.g., "guider.jk15")
        messenger: Serverish Messenger instance
        parent: Parent MonitoredObject (optional, for internal hierarchy)
        check_interval: Heartbeat interval in seconds (default: 10.0)
        healthcheck_interval: Health check interval in seconds (default: 30.0)
        subject_prefix: NATS subject prefix (default: "svc")
        parent_name: Optional parent name for grouping in displays
    """

    def __init__(
        self,
        name: str,
        messenger,
        parent: MonitoredObject | None = None,
        check_interval: float = 10.0,
        healthcheck_interval: float = 30.0,
        subject_prefix: str = "svc",
        parent_name: str | None = None,
    ):
        super().__init__(name, parent, check_interval, healthcheck_interval)
        self.messenger = messenger
        self.subject_prefix = subject_prefix
        self.parent_name = parent_name

        self._cached_pid = os.getpid()
        self._cached_hostname = socket.gethostname()

        self._status_publisher = None
        self._heartbeat_publisher = None

        if messenger is not None:
            status_subject = f"{self.subject_prefix}.status.{self.name}"
            self._status_publisher = get_publisher(status_subject)

            heartbeat_subject = f"{self.subject_prefix}.heartbeat.{self.name}"
            self._heartbeat_publisher = get_publisher(heartbeat_subject)

    async def __aenter__(self):
        """Enter context: start monitoring and send registration."""
        await self.start_monitoring()
        await self.send_registration()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit context: send shutdown and stop monitoring."""
        await self.send_shutdown()
        await self.stop_monitoring()
        return False

    def _on_status_changed(self):
        """Trigger immediate status send on status change."""
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._send_status_report())
        except RuntimeError:
            # No running event loop — skip status send
            pass

    async def _send_status_report(self):
        """Send status report to NATS."""
        if self._status_publisher is None:
            self.logger.debug("Status publisher not set, cannot send status report")
            return
        try:
            report = await self.get_full_report()
            data = report.to_dict()
            if self.parent_name:
                data["parent"] = self.parent_name
            data["pid"] = self._cached_pid
            data["hostname"] = self._cached_hostname
            await self._status_publisher.publish(data=data)
            self.logger.debug(f"Sent STATUS report to {self._status_publisher.subject}")
        except Exception as e:
            self.logger.error(f"Failed to send STATUS report: {e}")

    async def _send_heartbeat(self):
        """Send heartbeat to NATS."""
        if self._heartbeat_publisher is None:
            self.logger.debug("Heartbeat publisher not set, cannot send heartbeat")
            return
        try:
            data = {
                "service_id": self.name,
                "timestamp": dt_utcnow_array(),
                "status": self.get_status().value,
            }
            await self._heartbeat_publisher.publish(data=data)
            self.logger.debug(f"Sent HEARTBEAT to {self._heartbeat_publisher.subject}")
        except Exception as e:
            self.logger.error(f"Failed to send HEARTBEAT: {e}")

    async def send_registration(self):
        """No-op: registry events are published by runners, not monitors."""
        self.logger.debug("send_registration() called (no-op — runners handle registry events)")

    async def send_shutdown(self):
        """No-op: registry events are published by runners, not monitors."""
        self.logger.debug("send_shutdown() called (no-op — runners handle registry events)")
