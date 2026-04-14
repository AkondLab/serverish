"""Tests for serverish.monitoring module.

Tests status enum, status reports, aggregation, monitored object hierarchy,
task tracking, health checks, metric callbacks, and the factory function.
"""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

from serverish.base.status import Status as DiagStatus, StatusEnum
from serverish.monitoring import (
    Status,
    StatusReport,
    aggregate_status,
    MonitoredObject,
    ReportingMonitoredObject,
    MessengerMonitoredObject,
    DummyMonitoredObject,
    create_monitor,
    diagnostic_to_monitoring_status,
    diagnostics_to_status,
    health_status_metric_cb,
    bind_diagnostics,
)


# --- Status Enum ---

class TestStatus:
    def test_status_values(self):
        assert Status.OK.value == "ok"
        assert Status.FAILED.value == "failed"
        assert Status.UNKNOWN.value == "unknown"

    def test_status_str(self):
        assert str(Status.OK) == "ok"
        assert str(Status.FAILED) == "failed"

    def test_is_healthy(self):
        healthy = {Status.OK, Status.IDLE, Status.BUSY, Status.DEGRADED, Status.WARNING}
        unhealthy = {Status.UNKNOWN, Status.STARTUP, Status.ERROR, Status.SHUTDOWN, Status.FAILED}

        for s in healthy:
            assert s.is_healthy, f"{s} should be healthy"
        for s in unhealthy:
            assert not s.is_healthy, f"{s} should not be healthy"

    def test_is_operational(self):
        operational = {Status.STARTUP, Status.OK, Status.IDLE, Status.BUSY, Status.DEGRADED, Status.WARNING}
        non_operational = {Status.UNKNOWN, Status.ERROR, Status.SHUTDOWN, Status.FAILED}

        for s in operational:
            assert s.is_operational, f"{s} should be operational"
        for s in non_operational:
            assert not s.is_operational, f"{s} should not be operational"


# --- StatusReport ---

class TestStatusReport:
    def test_create_with_defaults(self):
        report = StatusReport(name="test", status=Status.OK)
        assert report.name == "test"
        assert report.status == Status.OK
        assert report.message is None
        assert report.timestamp is not None
        assert len(report.timestamp) == 7

    def test_to_dict_minimal(self):
        report = StatusReport(name="test", status=Status.OK)
        d = report.to_dict()
        assert d["name"] == "test"
        assert d["status"] == "ok"
        assert "timestamp" in d
        assert "message" not in d
        assert "details" not in d

    def test_to_dict_full(self):
        report = StatusReport(
            name="test",
            status=Status.ERROR,
            message="Something broke",
            details={"key": "value"},
            parent="parent_svc",
        )
        d = report.to_dict()
        assert d["message"] == "Something broke"
        assert d["details"] == {"key": "value"}
        assert d["parent"] == "parent_svc"

    def test_roundtrip(self):
        original = StatusReport(
            name="test",
            status=Status.WARNING,
            message="High load",
            details={"cpu": 95},
            parent="parent",
        )
        d = original.to_dict()
        restored = StatusReport.from_dict(d)
        assert restored.name == original.name
        assert restored.status == original.status
        assert restored.message == original.message
        assert restored.details == original.details
        assert restored.parent == original.parent

    def test_get_timestamp_dt(self):
        report = StatusReport(name="test", status=Status.OK)
        dt = report.get_timestamp_dt()
        assert dt is not None

    def test_from_dict_missing_fields(self):
        with pytest.raises(KeyError, match="name"):
            StatusReport.from_dict({"status": "ok"})
        with pytest.raises(KeyError, match="status"):
            StatusReport.from_dict({"name": "test"})

    def test_from_dict_invalid_status(self):
        with pytest.raises(ValueError):
            StatusReport.from_dict({"name": "test", "status": "not_a_status"})

    def test_get_timestamp_dt_none(self):
        report = StatusReport(name="test", status=Status.OK, timestamp=None)
        # __post_init__ fills in timestamp, so override it
        report.timestamp = None
        assert report.get_timestamp_dt() is None


# --- Aggregation ---

class TestAggregation:
    def test_empty(self):
        assert aggregate_status([]) == Status.UNKNOWN

    def test_single_ok(self):
        assert aggregate_status([StatusReport("a", Status.OK)]) == Status.OK

    def test_failed_dominates(self):
        reports = [StatusReport("a", Status.OK), StatusReport("b", Status.FAILED)]
        assert aggregate_status(reports) == Status.FAILED

    def test_error_dominates_over_warning(self):
        reports = [StatusReport("a", Status.WARNING), StatusReport("b", Status.ERROR)]
        assert aggregate_status(reports) == Status.ERROR

    def test_warning_dominates_over_degraded(self):
        reports = [StatusReport("a", Status.DEGRADED), StatusReport("b", Status.WARNING)]
        assert aggregate_status(reports) == Status.WARNING

    def test_busy_when_mixed_ok_busy(self):
        reports = [StatusReport("a", Status.OK), StatusReport("b", Status.BUSY)]
        assert aggregate_status(reports) == Status.BUSY

    def test_idle_when_mixed_ok_idle(self):
        reports = [StatusReport("a", Status.OK), StatusReport("b", Status.IDLE)]
        assert aggregate_status(reports) == Status.IDLE

    def test_all_ok(self):
        reports = [StatusReport("a", Status.OK), StatusReport("b", Status.OK)]
        assert aggregate_status(reports) == Status.OK

    def test_precedence_chain(self):
        """Full precedence: FAILED > ERROR > WARNING > DEGRADED > STARTUP > SHUTDOWN > BUSY"""
        precedence = [
            Status.FAILED, Status.ERROR, Status.WARNING,
            Status.DEGRADED, Status.STARTUP, Status.SHUTDOWN, Status.BUSY,
        ]
        for i, higher in enumerate(precedence[:-1]):
            lower = precedence[i + 1]
            reports = [StatusReport("a", lower), StatusReport("b", higher)]
            result = aggregate_status(reports)
            assert result == higher, f"{higher} should dominate over {lower}, got {result}"


# --- MonitoredObject ---

class TestMonitoredObject:
    def test_initial_status(self):
        obj = MonitoredObject("test")
        assert obj.get_status() == Status.UNKNOWN
        assert obj.name == "test"

    def test_set_status(self):
        obj = MonitoredObject("test")
        obj.set_status(Status.OK, "Running")
        assert obj.get_status() == Status.OK

    def test_parent_child_registration(self):
        parent = MonitoredObject("parent")
        child = MonitoredObject("child", parent=parent)
        assert "child" in parent.children
        assert child.parent is parent

    def test_add_remove_submonitor(self):
        parent = MonitoredObject("parent")
        child = MonitoredObject("child")
        parent.add_submonitor(child)
        assert "child" in parent.children
        parent.remove_submonitor("child")
        assert "child" not in parent.children
        assert child.parent is None

    def test_cancel_error_status_with_task_tracking(self):
        obj = MonitoredObject("test")
        obj._task_tracking_enabled = True
        obj._active_tasks = 0
        obj.set_status(Status.ERROR, "Something failed")
        obj.cancel_error_status()
        assert obj.get_status() == Status.IDLE

    def test_cancel_error_status_without_task_tracking(self):
        obj = MonitoredObject("test")
        obj.set_status(Status.ERROR, "Something failed")
        obj.cancel_error_status()
        assert obj.get_status() == Status.OK

    def test_cancel_error_status_noop_when_healthy(self):
        obj = MonitoredObject("test")
        obj.set_status(Status.OK, "Fine")
        obj.cancel_error_status()
        assert obj.get_status() == Status.OK

    @pytest.mark.asyncio
    async def test_healthcheck_no_callbacks(self):
        obj = MonitoredObject("test")
        obj.set_status(Status.OK)
        status = await obj.healthcheck()
        assert status == Status.OK

    @pytest.mark.asyncio
    async def test_healthcheck_sync_callback(self):
        obj = MonitoredObject("test")
        obj.set_status(Status.OK)
        obj.add_healthcheck_cb(lambda: Status.ERROR)
        status = await obj.healthcheck()
        assert status == Status.ERROR

    @pytest.mark.asyncio
    async def test_healthcheck_async_callback_unhealthy(self):
        obj = MonitoredObject("test")
        obj.set_status(Status.OK)

        async def check():
            return Status.ERROR

        obj.add_healthcheck_cb(check)
        status = await obj.healthcheck()
        assert status == Status.ERROR

    @pytest.mark.asyncio
    async def test_healthcheck_async_callback_degraded_is_healthy(self):
        """DEGRADED is considered healthy per Status.is_healthy."""
        obj = MonitoredObject("test")
        obj.set_status(Status.OK)

        async def check():
            return Status.DEGRADED

        obj.add_healthcheck_cb(check)
        status = await obj.healthcheck()
        assert status == Status.OK  # DEGRADED is healthy, so returns current status

    @pytest.mark.asyncio
    async def test_healthcheck_healthy_callback(self):
        obj = MonitoredObject("test")
        obj.set_status(Status.OK)
        obj.add_healthcheck_cb(lambda: None)
        status = await obj.healthcheck()
        assert status == Status.OK

    @pytest.mark.asyncio
    async def test_healthcheck_callback_exception(self):
        obj = MonitoredObject("test")
        obj.set_status(Status.OK)

        def broken():
            raise RuntimeError("check failed")

        obj.add_healthcheck_cb(broken)
        status = await obj.healthcheck()
        assert status == Status.ERROR

    @pytest.mark.asyncio
    async def test_metric_callbacks(self):
        obj = MonitoredObject("test")
        obj.set_status(Status.OK)
        obj.add_metric_cb(lambda: {"queue_size": 42})

        async def async_metrics():
            return {"latency_ms": 5.0}

        obj.add_metric_cb(async_metrics)

        report = await obj.get_full_report()
        assert report.details["metrics"]["queue_size"] == 42
        assert report.details["metrics"]["latency_ms"] == 5.0

    @pytest.mark.asyncio
    async def test_full_report_with_children(self):
        parent = MonitoredObject("parent")
        child1 = MonitoredObject("child1", parent=parent)
        child2 = MonitoredObject("child2", parent=parent)

        parent.set_status(Status.OK)
        child1.set_status(Status.OK)
        child2.set_status(Status.ERROR)

        report = await parent.get_full_report()
        assert report.status == Status.ERROR  # Aggregated from children
        assert report.details is not None
        assert report.details["own_status"] == "ok"
        assert len(report.details["children"]) == 2

    @pytest.mark.asyncio
    async def test_full_report_no_children(self):
        obj = MonitoredObject("test")
        obj.set_status(Status.OK, "Running fine")
        report = await obj.get_full_report()
        assert report.status == Status.OK
        assert report.details is None


# --- Task Tracking ---

class TestTaskTracking:
    @pytest.mark.asyncio
    async def test_task_enables_tracking(self):
        obj = MonitoredObject("test")
        obj.set_status(Status.OK)
        assert not obj._task_tracking_enabled

        async with obj.track_task("work"):
            assert obj._task_tracking_enabled
            assert obj.get_status() == Status.BUSY

    @pytest.mark.asyncio
    async def test_task_transitions_to_idle(self):
        obj = MonitoredObject("test")
        obj.set_status(Status.OK)

        async with obj.track_task("work"):
            assert obj.get_status() == Status.BUSY

        # Wait for delayed IDLE transition (1s + margin)
        await asyncio.sleep(1.5)
        assert obj.get_status() == Status.IDLE

    @pytest.mark.asyncio
    async def test_overlapping_tasks_stay_busy(self):
        obj = MonitoredObject("test")
        obj.set_status(Status.OK)

        async with obj.track_task("work1"):
            assert obj.get_status() == Status.BUSY
            async with obj.track_task("work2"):
                assert obj._active_tasks == 2
            # work2 done, but work1 still running
            assert obj.get_status() == Status.BUSY

    @pytest.mark.asyncio
    async def test_task_does_not_override_error(self):
        obj = MonitoredObject("test")
        obj.set_status(Status.ERROR, "Something broken")

        async with obj.track_task("work"):
            assert obj.get_status() == Status.ERROR  # Should NOT change to BUSY


# --- ReportingMonitoredObject ---

class TestReportingMonitoredObject:
    @pytest.mark.asyncio
    async def test_start_stop_monitoring(self):
        obj = ReportingMonitoredObject("test", check_interval=0.1, healthcheck_interval=0.2)
        await obj.start_monitoring()
        assert obj._running
        assert obj._heartbeat_task is not None
        assert obj._healthcheck_task is not None

        await asyncio.sleep(0.3)  # Let loops run a bit

        await obj.stop_monitoring()
        assert not obj._running
        assert obj._heartbeat_task is None
        assert obj._healthcheck_task is None

    @pytest.mark.asyncio
    async def test_double_start_is_safe(self):
        obj = ReportingMonitoredObject("test", check_interval=1.0)
        await obj.start_monitoring()
        task1 = obj._heartbeat_task
        await obj.start_monitoring()  # Should be no-op
        assert obj._heartbeat_task is task1
        await obj.stop_monitoring()

    @pytest.mark.asyncio
    async def test_context_manager(self):
        obj = ReportingMonitoredObject("test", check_interval=1.0)
        async with obj:
            assert obj._running
        assert not obj._running

    @pytest.mark.asyncio
    async def test_healthcheck_loop_updates_status(self):
        obj = ReportingMonitoredObject("test", check_interval=1.0, healthcheck_interval=0.1)
        obj.set_status(Status.OK)

        call_count = 0

        def error_check():
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                return Status.ERROR  # ERROR is unhealthy, triggers update
            return None

        obj.add_healthcheck_cb(error_check)

        async with obj:
            await asyncio.sleep(0.5)

        assert obj.get_status() == Status.ERROR


# --- DummyMonitoredObject ---

class TestDummyMonitoredObject:
    @pytest.mark.asyncio
    async def test_context_manager(self):
        obj = DummyMonitoredObject("test")
        async with obj:
            obj.set_status(Status.OK, "Running")
            assert obj.get_status() == Status.OK

    @pytest.mark.asyncio
    async def test_task_tracking_works(self):
        """DummyMonitoredObject inherits task tracking from MonitoredObject."""
        obj = DummyMonitoredObject("test")
        obj.set_status(Status.OK)
        async with obj.track_task("work"):
            assert obj.get_status() == Status.BUSY


# --- Factory ---

class TestCreateMonitor:
    @pytest.mark.asyncio
    async def test_creates_appropriate_monitor(self):
        """create_monitor returns a MonitoredObject (specific type depends on Messenger state)."""
        monitor = await create_monitor("test_factory")
        assert isinstance(monitor, MonitoredObject)
        assert monitor.name == "test_factory"

    @pytest.mark.asyncio
    async def test_generates_name_when_none(self):
        monitor = await create_monitor()
        assert isinstance(monitor, MonitoredObject)
        assert monitor.name  # Should have auto-generated name

    def test_messenger_monitor_direct_creation(self):
        """MessengerMonitoredObject can be created directly with None messenger."""
        monitor = MessengerMonitoredObject(
            name="direct_test",
            messenger=None,
            subject_prefix="test",
        )
        assert monitor.name == "direct_test"
        assert monitor._status_publisher is None
        assert monitor._heartbeat_publisher is None

    def test_dummy_monitor_is_valid_api(self):
        """DummyMonitoredObject provides the full MonitoredObject API."""
        dummy = DummyMonitoredObject("dummy")
        assert isinstance(dummy, MonitoredObject)
        dummy.set_status(Status.OK, "Running")
        assert dummy.get_status() == Status.OK
        dummy.add_healthcheck_cb(lambda: None)
        dummy.add_metric_cb(lambda: {"key": "val"})


# --- Bridge (base ↔ monitoring integration) ---

class TestBridge:
    def test_diagnostic_ok_to_monitoring(self):
        result = diagnostic_to_monitoring_status(DiagStatus.new_ok("All good"))
        assert result == Status.OK

    def test_diagnostic_fail_to_monitoring(self):
        result = diagnostic_to_monitoring_status(DiagStatus.new_fail("Connection lost"))
        assert result == Status.ERROR

    def test_diagnostic_disabled_to_monitoring(self):
        result = diagnostic_to_monitoring_status(DiagStatus.new_disabled("Turned off"))
        assert result == Status.SHUTDOWN

    def test_diagnostic_na_to_monitoring(self):
        result = diagnostic_to_monitoring_status(DiagStatus.new_na("Not applicable"))
        assert result == Status.UNKNOWN

    def test_diagnostics_all_ok(self):
        results = {
            "ping": DiagStatus.new_ok("Ping OK"),
            "dns": DiagStatus.new_ok("DNS OK"),
        }
        assert diagnostics_to_status(results) == Status.OK

    def test_diagnostics_with_failure(self):
        results = {
            "ping": DiagStatus.new_ok("Ping OK"),
            "nats": DiagStatus.new_fail("Connection refused"),
        }
        assert diagnostics_to_status(results) == Status.ERROR

    def test_diagnostics_empty(self):
        assert diagnostics_to_status({}) == Status.UNKNOWN

    def test_diagnostics_mixed_ok_na(self):
        results = {
            "ping": DiagStatus.new_ok("OK"),
            "dns": DiagStatus.new_na("Skipped"),
        }
        assert diagnostics_to_status(results) == Status.OK

    def test_health_status_metric_cb(self):
        """health_status_metric_cb wraps a component's health_status property."""

        class FakePublisher:
            @property
            def health_status(self):
                return {"publish_count": 42, "error_count": 0}

        cb = health_status_metric_cb(FakePublisher())
        result = cb()
        assert result["publish_count"] == 42
        assert result["error_count"] == 0

    def test_health_status_metric_cb_handles_error(self):
        """Gracefully handles components that fail to report health."""

        class BrokenComponent:
            @property
            def health_status(self):
                raise RuntimeError("not available")

        cb = health_status_metric_cb(BrokenComponent())
        result = cb()
        assert result == {}

    @pytest.mark.asyncio
    async def test_bind_diagnostics(self):
        """bind_diagnostics registers a healthcheck that calls diagnose()."""

        class FakeHasStatuses:
            async def diagnose(self):
                return {"ping": DiagStatus.new_fail("Timeout")}

        monitor = MonitoredObject("test_bind")
        monitor.set_status(Status.OK)
        bind_diagnostics(monitor, FakeHasStatuses())

        # Now healthcheck should detect the failure
        status = await monitor.healthcheck()
        assert status == Status.ERROR

    @pytest.mark.asyncio
    async def test_bind_diagnostics_healthy(self):
        """When diagnostics are OK, healthcheck returns current status."""

        class FakeHasStatuses:
            async def diagnose(self):
                return {"ping": DiagStatus.new_ok("OK")}

        monitor = MonitoredObject("test_bind_ok")
        monitor.set_status(Status.IDLE)
        bind_diagnostics(monitor, FakeHasStatuses())

        status = await monitor.healthcheck()
        assert status == Status.IDLE  # No override when healthy

    @pytest.mark.asyncio
    async def test_health_status_as_monitor_metric(self):
        """Full integration: health_status dict appears in monitor report metrics."""

        class FakeReader:
            @property
            def health_status(self):
                return {"messages_received": 100, "pending_messages": 5}

        monitor = MonitoredObject("test_metrics_integration")
        monitor.set_status(Status.OK)
        monitor.add_metric_cb(health_status_metric_cb(FakeReader()))

        report = await monitor.get_full_report()
        assert report.details["metrics"]["messages_received"] == 100
        assert report.details["metrics"]["pending_messages"] == 5
