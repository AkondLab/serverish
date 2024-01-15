"""Simple example of telemetry data publishing using serverish.Messenger"""

import asyncio
import datetime
import logging

from serverish.base import dt_utcnow_array
from serverish.messenger import Messenger, get_publisher, get_reader


async def do():

    reader = get_reader('telemetry.weather.davis',
                        deliver_policy='by_start_time',
                        opt_start_time=datetime.datetime.now() - datetime.timedelta(days=1))
    async for data, meta in reader:
        print(data['measurements']['wind_10min_ms'])

host = '192.168.7.38'
port = 4222


async def main():
    msg = Messenger()

    async with msg.context(host, port) as msg:
        await do()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    asyncio.run(main())
