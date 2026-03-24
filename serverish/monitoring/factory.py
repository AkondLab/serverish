"""Factory function for creating monitors with auto-detection.

create_monitor() selects the appropriate implementation:
    - MessengerMonitoredObject when NATS/Messenger is available
    - DummyMonitoredObject for graceful degradation without NATS

To be extracted and generalized from ocabox-tcs create_monitor().
"""

from __future__ import annotations
