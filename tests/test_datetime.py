import pytest
from datetime import datetime
from serverish.base import dt_to_array, dt_from_array, dt_ensure_array, dt_ensure_datetime, dt_utcnow_array


def test_dt_to_array():
    dt = datetime(2023, 9, 12, 10, 5, 30, 123456)
    assert dt_to_array(dt) == [2023, 9, 12, 10, 5, 30, 123456]

def test_dt_from_array():
    arr = [2023, 9, 12, 10, 5, 30, 123456]
    assert dt_from_array(arr) == datetime(2023, 9, 12, 10, 5, 30, 123456)

def test_dt_ensure_array_with_datetime():
    dt = datetime(2023, 9, 12, 10, 5, 30, 123456)
    assert dt_ensure_array(dt) == [2023, 9, 12, 10, 5, 30, 123456]

def test_dt_ensure_array_with_array():
    arr = [2023, 9, 12, 10, 5, 30, 123456]
    assert dt_ensure_array(arr) == arr

def test_dt_ensure_datetime_with_datetime():
    dt = datetime(2023, 9, 12, 10, 5, 30, 123456)
    assert dt_ensure_datetime(dt) == dt

def test_dt_ensure_datetime_with_array():
    arr = [2023, 9, 12, 10, 5, 30, 123456]
    assert dt_ensure_datetime(arr) == datetime(2023, 9, 12, 10, 5, 30, 123456)

def test_now_array():
    before = datetime.utcnow()
    result = dt_utcnow_array()
    after = datetime.utcnow()

    # Wartości z before i after są granicami dla wartości z now_array
    assert before <= datetime(*result) <= after
