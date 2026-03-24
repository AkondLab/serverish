"""Heartbeat protocol over NATS JetStream.

Periodic lightweight signals published on configurable intervals.
Consumers can detect stale/dead services from missing heartbeats.

To be extracted and generalized from ocabox-tcs heartbeat implementation.
"""

from __future__ import annotations
