"""Health check loop and metric callback infrastructure.

Provides periodic health verification with configurable intervals
and support for both sync and async check callbacks.

To be extracted and generalized from ocabox-tcs health check patterns.
"""

from __future__ import annotations
