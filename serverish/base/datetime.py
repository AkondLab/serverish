""" Those functions are used to convert datetime objects to array representation
(list similar to time tuple) and vice versa.

This repetition is chosen for serializing datetime to JSON as fast and easy to process."""

from __future__ import annotations

from time import struct_time, mktime
from datetime import datetime
from typing import Sequence


def dt_to_array(dt: datetime | None) -> list | None:
    if dt is None:
        return None
    else:
        return [dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond]


def dt_from_array(t: Sequence | None) -> datetime | None:
    if not t:
        return None
    return datetime(*t)


def dt_ensure_array(dt: datetime | Sequence | None) -> list | None:
    if dt is None:
        return None
    elif isinstance(dt, datetime):
        return dt_to_array(dt)
    else:
        return list(dt)


def dt_ensure_datetime(dt: datetime | Sequence | None) -> datetime | None:
    if isinstance(dt, datetime) or dt is None:
        return dt
    else:
        return dt_from_array(dt)

def dt_utcnow_array() -> list:
    dt = datetime.utcnow()
    return dt_to_array(dt)