import asyncio
import time

from serverish.messenger import Messenger, get_publisher


async def do():
    t = 22.5

    pub = await get_publisher()
    await pub.publish(data={
        'ts': list(time.gmtime()),
        'measurements': {
            'temp': t  # Example temperature measurement
        }
    })


async def main():
    msg = Messenger()
    await msg.open()

    await do()

    await msg.close()


if __name__ == '__main__':
    asyncio.run(main())