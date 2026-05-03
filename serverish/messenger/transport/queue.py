"""Thread-safe transport queue with backpressure strategies.

Queue lives in the NATS loop thread. User loop consumes via
cross-thread coroutine scheduling (run_coroutine_threadsafe).

Backpressure strategies (from doc/nats-client-considerations.md §6.3):
    - Drop: bounded queue, overwrite/drop oldest (for UI/telemetry)
    - Pause pull: stop sending .NEXT, let JetStream buffer
    - Spill: durable buffer for critical data

See doc/nats-client-considerations.md §6 for queue architecture.
"""

from __future__ import annotations
