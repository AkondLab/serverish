import asyncio
import logging
import time
from typing import Callable

from serverish.base import dt_utcnow_array
from serverish.messenger import Messenger, get_publisher, get_reader, get_callbacksubscriber

logger = logging.getLogger(__name__.rsplit('.')[-1])

class SendMsg:

    def __init__(self):
        self.m = Messenger()
        super().__init__()

    async def run_test(self):
        await self.conn()
        await self.subscr(subject='test.dev.fits_download', callb=self.callb)
        w=0
        while True:
            if w == 2 or w == 4 or w == 6:
                await self.send(subject='test.dev.fits_download',
                                data={
                                    'fits_id': '1234142c_test',
                                    'header': {'header': 1,
                                               'header2': 333
                                               },
                                    'ts': dt_utcnow_array(),
                                },
                                meta={
                                })
            await asyncio.sleep(3)
            w += 1

    async def conn(self):
        self._loop = asyncio.get_running_loop()
        try:
            addr = '192.168.8.140'
            addr = 'localhost'
            port = 4222
            await self.m.open(host = addr, port = port)
            logger.info(f'Nats connected to {addr}:{port}')
            self.connected = True
        except Exception as e:
            logger.warning(f'{e}')

    async def send(self, subject: str, data, meta):
        logger.info(f'Data send: {data}')
        pub = await get_publisher(subject=subject)
        await pub.publish(data=data, meta=meta)

    def callb(self, d1, d2) -> bool:
        logger.warning(f'callback data {d1}')
        logger.warning(f'callback meta {d2}')
        return True

    async def subscr(self, subject: str, callb: Callable):
        try:
            csub = await get_callbacksubscriber(subject=subject)
            await csub.subscribe(callback=callb)
            logger.info(f'Subscription for {subject} started')
        except Exception as e:
            logger.warning(f'{e}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
    s = SendMsg()
    asyncio.run(s.run_test())