"""Configurable ACK strategies for JetStream consumers.

From doc/nats-client-considerations.md §7:

    ACK-after-enqueue (default for telemetry/UI):
        - Immediate ACK in NATS loop after enqueue
        - Stable, no redelivery storms
        - Possible data loss unless queue is durable

    ACK-after-process (default for jobs/tasks):
        - ACK triggered by user code after processing
        - At-least-once delivery guarantee
        - Depends on user loop responsiveness

    Hybrid (recommended):
        - Configurable per use case
        - Telemetry/UI: enqueue-ack
        - Critical jobs: process-ack
"""

from __future__ import annotations
