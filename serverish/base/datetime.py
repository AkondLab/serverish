""" Those functions are used to convert datetime objects to array representation
(7-element list, similar to time tuple) and vice versa.

This representation is chosen for serializing datetime to JSON as fast and easy to process.
We are using list instead of tuple for smooth conversion to/from JSON.

The meaning of the elements of the array is as follows:
[year, month, day, hour, minute, second, microsecond]
Time is in UTC.
This is compatible with the datetime constructor, e.g.:
   dt = datetime(*array, tzinfo=timezone.utc)
but you have also the convenience function:
    dt = dt_from_array(array)
to do the same preserving None.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Sequence


def dt_to_array(dt: datetime | None) -> list | None:
    if dt is None:
        return None
    else:
        dt = dt.astimezone(timezone.utc)
        return [dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond]


def dt_from_array(t: Sequence | None) -> datetime | None:
    if not t:
        return None
    try:
        return datetime(*t, tzinfo=timezone.utc)
    except TypeError:  # Old 9-position format?
        return datetime(*t[:7])


def dt_ensure_array(dt: datetime | Sequence | float | None) -> list | None:
    """Supports datetime, array, float (POSIX timestamp) and None"""
    if dt is None:
        return None
    elif isinstance(dt, datetime):
        return dt_to_array(dt)
    elif isinstance(dt, float):
        return dt_to_array(datetime.fromtimestamp(dt, timezone.utc))
    else:
        return list(dt)


def dt_ensure_datetime(dt: datetime | Sequence | float | None) -> datetime | None:
    """Supports datetime, array, float (POSIX timestamp) and None"""
    if isinstance(dt, datetime) or dt is None:
        return dt
    elif isinstance(dt, float):
        return datetime.fromtimestamp(dt, timezone.utc)
    else:
        return dt_from_array(dt)


def dt_utcnow_array() -> list:
    dt = datetime.now(timezone.utc)
    return dt_to_array(dt)


