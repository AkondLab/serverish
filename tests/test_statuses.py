from serverish.base.status import Status, StatusReport, aggregate_status


def test_status_enum_members():
    """Status enum has exactly 10 members."""
    assert len(Status) == 10
    expected = {'UNKNOWN', 'STARTUP', 'OK', 'IDLE', 'BUSY', 'DEGRADED', 'WARNING', 'ERROR', 'SHUTDOWN', 'FAILED'}
    assert {s.name for s in Status} == expected


def test_status_is_healthy():
    """is_healthy returns True for OK, IDLE, BUSY, DEGRADED, WARNING."""
    healthy = {Status.OK, Status.IDLE, Status.BUSY, Status.DEGRADED, Status.WARNING}
    for s in Status:
        assert s.is_healthy == (s in healthy), f"{s.name}.is_healthy should be {s in healthy}"


def test_status_is_operational():
    """is_operational returns True for STARTUP, OK, IDLE, BUSY, DEGRADED, WARNING."""
    operational = {Status.STARTUP, Status.OK, Status.IDLE, Status.BUSY, Status.DEGRADED, Status.WARNING}
    for s in Status:
        assert s.is_operational == (s in operational), f"{s.name}.is_operational should be {s in operational}"


def test_status_str():
    """str(Status.OK) returns 'ok'."""
    assert str(Status.OK) == "ok"
    assert str(Status.FAILED) == "failed"


def test_status_report_factory_defaults():
    """Factory methods produce correct deduce_other defaults."""
    assert StatusReport.ok().deduce_other is True
    assert StatusReport.error().deduce_other is False
    assert StatusReport.failed().deduce_other is False
    assert StatusReport.unknown().deduce_other is False
    assert StatusReport.shutdown().deduce_other is False
    assert StatusReport.degraded().deduce_other is False
    assert StatusReport.warning().deduce_other is False


def test_status_report_factory_statuses():
    """Factory methods produce correct Status values."""
    assert StatusReport.ok().status == Status.OK
    assert StatusReport.error().status == Status.ERROR
    assert StatusReport.failed().status == Status.FAILED
    assert StatusReport.unknown().status == Status.UNKNOWN
    assert StatusReport.shutdown().status == Status.SHUTDOWN
    assert StatusReport.degraded().status == Status.DEGRADED
    assert StatusReport.warning().status == Status.WARNING


def test_status_report_deduced():
    """Deduced StatusReport preserves source status."""
    src = StatusReport.ok('source')
    ded = StatusReport.deduced(src)
    assert ded.status == Status.OK
    assert ded.message == "Deduced"
    assert ded.deduce_other is False


def test_status_report_no_bool():
    """StatusReport has no __bool__ — explicit checks required."""
    assert '__bool__' not in StatusReport.__dict__


def test_status_report_serialization_roundtrip():
    """to_dict/from_dict round-trip preserves all fields."""
    r = StatusReport(name='test', status=Status.OK, message='hello')
    d = r.to_dict()
    r2 = StatusReport.from_dict(d)
    assert r2.name == 'test'
    assert r2.status == Status.OK
    assert r2.message == 'hello'


def test_status_report_serialization_deduce_other():
    """deduce_other=True is included in to_dict, False is omitted."""
    r_ok = StatusReport.ok('test')
    assert r_ok.deduce_other is True
    d = r_ok.to_dict()
    assert d.get('deduce_other') is True

    r_err = StatusReport.error('bad')
    d2 = r_err.to_dict()
    assert 'deduce_other' not in d2


def test_aggregate_status_empty():
    """aggregate_status of empty list returns UNKNOWN."""
    assert aggregate_status([]) == Status.UNKNOWN


def test_aggregate_status_precedence():
    """aggregate_status follows severity precedence."""
    assert aggregate_status([StatusReport.ok(), StatusReport.error()]) == Status.ERROR
    assert aggregate_status([StatusReport.ok(), StatusReport.warning()]) == Status.WARNING
    assert aggregate_status([StatusReport.ok(), StatusReport.degraded()]) == Status.DEGRADED
    assert aggregate_status([StatusReport.error(), StatusReport.failed()]) == Status.FAILED
    assert aggregate_status([StatusReport.ok()]) == Status.OK
