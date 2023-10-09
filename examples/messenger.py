"""Simple example of telemetry data publishing using serverish.Messenger"""
import logging
import asyncio
import logging
import time
from random import random

from serverish.base import dt_utcnow_array
from serverish.messenger import Messenger, get_publisher, get_reader

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s.%(msecs)03d [%(levelname)s] [%(name)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

host = 'localhost'
port = 4222
subject = 'test.messanerexample'


async def publisher(n=1000, dt=3.0):

    pub = get_publisher(subject)
    for _ in range(n):
        t = 20.0 + 10 * random()
        await pub.publish(
            data={
                'ts': dt_utcnow_array(),
                'measurements': {
                    'temp': t  # Example temperature measurement
                }
            },
            meta={
                'trace_level': logging.WARN,
            }
        )
        await asyncio.sleep(dt)


async def subscriber():
    sub = get_reader(subject, deliver_policy='all')
    async for msg, meta in sub:
        pass



async def main1():
    msg = Messenger()

    await msg.open(host, port)
    try:
        await msg.purge(subject)
        # pre-publish
        await publisher(10, dt=0.3)
        await asyncio.gather(
            subscriber(),
            publisher(1000, dt=3)

        )

        await publisher(1000)
    finally:
        print('closing!!')
        await msg.close()
        print('closed...')



if __name__ == '__main__':
    # both versions are ok
    asyncio.run(main1())
    # asyncio.run(main2())
