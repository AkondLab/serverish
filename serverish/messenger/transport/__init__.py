"""serverish.messenger.transport — Thread-separated NATS transport layer.

Isolates NATS I/O from the user's asyncio event loop to prevent
event loop starvation, ACK starvation, and pull lifecycle breaks.

Architecture (from doc/nats-client-considerations.md):

    NATS loop (dedicated thread):
        - Socket I/O, parsing
        - Pull requests
        - Queue enqueue
        - ACK (in enqueue-ack mode)

    User loop (application thread):
        - Message processing
        - async for iteration
        - ACK (in process-ack mode)

Core invariant: User code never blocks the NATS loop.

Key components:
    NatsLoop        — Dedicated thread running NATS I/O event loop
    TransportQueue  — Thread-safe message queue with backpressure
    PullManager     — Pull consumer lifecycle with reconnect handling
    AckStrategy     — Configurable ACK policies (enqueue-ack, process-ack, hybrid)
"""

from __future__ import annotations
