"""Pull consumer lifecycle management with reconnect handling.

Manages the pull request loop, consumer creation/recreation,
and reconnection strategy including inbox invalidation.

On reconnect (from doc/nats-client-considerations.md §9):
    1. Invalidate inbox and pull state
    2. Restart pull loop and iterator bridge
    3. Ensure consumer exists (idempotent)

Also handles consumer scaling via fan-in pattern
(single consumer with FilterSubjects or wildcard + local routing).
"""

from __future__ import annotations
