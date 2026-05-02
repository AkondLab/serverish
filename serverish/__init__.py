"""Serverish package init.

Defines a custom logging level ``CHECKING`` (numeric value 15) sitting
between ``INFO`` (20) and ``DEBUG`` (10). Used for high-volume diagnostic
logs around JetStream consumer lifecycle that would drown out INFO if
unconditional, but that we want to switch on without dropping to full
DEBUG.

Activation: caller sets the standard logging level on the relevant
logger (or root) to ``serverish.CHECKING``. The accompanying
``log.checking(...)`` method behaves like ``log.info`` / ``log.debug``
and is a no-op when the effective level is higher.
"""

from __future__ import annotations

import logging

CHECKING: int = 15

logging.addLevelName(CHECKING, 'CHECKING')


def _checking(self: logging.Logger, msg: str, *args, **kwargs) -> None:
    if self.isEnabledFor(CHECKING):
        self._log(CHECKING, msg, args, **kwargs)


# Attach as a method on Logger so any module can do ``log.checking(...)``.
logging.Logger.checking = _checking  # type: ignore[attr-defined]


__all__ = ['CHECKING']
