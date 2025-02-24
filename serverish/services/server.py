import logging
import asyncio
import signal
from wsgiref.simple_server import server_version

from serverish.services.servers.simple import SimpleServer


async def run(server_config: dict | None = None):
    """Starts server"""

    sv = SimpleServer()

    # handle stop signals
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig,
                                lambda s=sig: asyncio.create_task(sv.stop()))

    # Chose Server subclass, based on settings...
    await sv.run(server_config)


def main():
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.error(f'Error: {e}')