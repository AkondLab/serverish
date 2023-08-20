"""Simple example of telemetry data publishing using serverish.Messenger"""

import asyncio
import time

from serverish.messenger import Messenger, get_publisher


async def do():
    t = 22.5

    pub = await get_publisher('test.telemetrypubexmaple')
    await pub.publish(data={
        'ts': list(time.gmtime()),
        'measurements': {
            'temp': t  # Example temperature measurement
        }
    })


host = 'localhost'
port = 4222

async def main1():
    msg = Messenger()

    await msg.open([msg.create_url(host, port)])
    await do()
    await msg.close()


async def main2():
    msg = Messenger()

    async with msg.context([msg.create_url(host, port)]) as msg:
        await do()


if __name__ == '__main__':
    # both versions are ok
    asyncio.run(main1())
    # asyncio.run(main2())
