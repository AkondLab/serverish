"""Dedicated NATS I/O thread with its own event loop.

Runs all NATS socket operations in a separate thread to isolate
transport from user code that may block the event loop.

See doc/nats-client-considerations.md §5 for architecture details.
"""

from __future__ import annotations
