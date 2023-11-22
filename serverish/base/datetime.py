""" Those functions are used to convert datetime objects to array representation
(7-element list, similar to time tuple) and vice versa.

This representation is chosen for serializing datetime to JSON as fast and easy to process.
We are using list instead of tuple for smooth conversion to/from JSON.

The meaning of the elements of the array is as follows:
[year, month, day, hour, minute, second, microsecond]
This is compatible with the datetime constructor, e.g.:
   dt = datetime(*array)
but you have also the convenience function:
    dt = dt_from_array(array)
to do the same preserving None.
"""

from __future__ import annotations

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

