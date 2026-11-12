"""Warnings reported by many components at once, logged once

When the connection hiccups, every reader of an application notices it within
the same moment and each would log its own warning: a hundred readers, a
hundred identical lines. `BurstLog` lets the first one through and sums up
the rest.
"""
from __future__ import annotations

import asyncio
import logging
import time


class BurstLog:
    """Logs the first warning of a kind in full, the following ones of that kind as one summary line

    The first warning of a `kind` opens a window. Further warnings of the
    same kind within the window are logged at DEBUG level and counted; when
    the window closes a single WARNING tells how many there were.

    Args:
        logger: logger to write to
        window: length of the window in seconds
    """

    def __init__(self, logger: logging.Logger, window: float = 5.0) -> None:
        self.logger = logger
        self.window = window
        self._bursts: dict[str, _Burst] = {}

    def warning(self, kind: str, message: str) -> None:
        """Logs a warning being one of possibly many of the same `kind`

        Args:
            kind: what happened, the same text for every component reporting it (e.g. 'Consumer has gone')
            message: full message of this particular component
        """
        burst = self._bursts.get(kind)
        if burst is not None and time.monotonic() < burst.closes_at:
            burst.suppressed += 1
            self.logger.debug(message)
            return
        if burst is not None:  # the window passed and nobody summed it up (no running loop at that time)
            self._summarize(kind)
        self.logger.warning(message)
        burst = self._bursts[kind] = _Burst(closes_at=time.monotonic() + self.window)
        try:
            burst.timer = asyncio.get_running_loop().call_later(self.window, self._summarize, kind)
        except RuntimeError:  # no running loop: summed up by the next warning of that kind
            pass

    def _summarize(self, kind: str) -> None:
        burst = self._bursts.pop(kind, None)
        if burst is None:
            return
        if burst.timer is not None:
            burst.timer.cancel()
        if burst.suppressed:
            self.logger.warning(f"{kind}: {burst.suppressed} more such warning(s) within {self.window:g}s "
                                f"(each one logged at DEBUG level)")


class _Burst:
    __slots__ = ('closes_at', 'suppressed', 'timer')

    def __init__(self, closes_at: float) -> None:
        self.closes_at = closes_at
        self.suppressed = 0
        self.timer: asyncio.TimerHandle | None = None
