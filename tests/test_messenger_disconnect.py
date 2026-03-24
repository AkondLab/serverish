import asyncio
import datetime
import logging
import multiprocessing

import pytest

from serverish.base import Task
from serverish.messenger import Messenger, get_publisher, get_reader

speed = 1.0


def publisher_process(host='localhost', port=4222, subject='test.messenger.messenger_pub_sub_with_disconnect', sleep_time=0.1, final=True, n=10):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)8s] %(message)s (%(filename)s:%(lineno)s)')
    asyncio.run(publisher_async(host=host, port=port, subject=subject, sleep_time=sleep_time, final=final, n=n))


async def publisher_async(host='localhost', port=4222, subject='test.messenger.messenger_pub_sub_with_disconnect', sleep_time=0.1, final=True, n=10):
    logging.info('Sender started')
    async with Messenger().context(host=host, port=port) as mes:
        pub = get_publisher(subject=subject)
        await publisher_task(pub, n=n, sleep_time=sleep_time, final=final)
        await pub.close()
    logging.info('Sender finished')


async def publisher_task(pub, n = 100, sleep_time = 0.1, final=True):
    for i in range(n):
        try:
            await pub.publish(data={'n': i, 'final': False})
            logging.info(f'Just published message: {i}')
        except Exception as e:
            logging.error(e)
        await asyncio.sleep(sleep_time)
    await pub.publish(data={'n': n, 'final': final})
    logging.info(f'Just published message: {n}{" (final)" if final else ""}')


@pytest.mark.skip(reason="For manual run, with NATS disconnect only")
@pytest.mark.nats
async def test_messenger_pub_sub_with_disconnect(messenger, unique_subject, nats_server):

    subject = unique_subject

    now = datetime.datetime.now()

    async def subsciber_task(sub):
        logging.info('Start subscriber loop')
        async for data, meta in sub:
            logging.info(f'Received message: [{meta["receive_mode"]}] '
                         f'{data["n"]} {"(final)" if data["final"] else ""}')
            if data['final']:
                break


    sender_process = multiprocessing.Process(
        target=publisher_process,
        kwargs=dict(host=nats_server['host'], port=nats_server['port'], subject=subject)
    )

    async def disconnector_task(msgr: Messenger):
        await asyncio.sleep(0.5)
        logging.info('Disconnecting...')
        await msgr.connection.nc.close()
        logging.info('Disconnected')
        await asyncio.sleep(1)
        logging.info('Connecting...')
        await msgr.connection.connect()
        # check connection:
        logging.info('Checking new connection')
        stream_name = await msgr.connection.js.find_stream_name_by_subject(subject)
        logging.info(f'Connected again (stream: {stream_name})')

    await messenger.purge(subject)
    logging.info('Purged')
    await asyncio.sleep(0.5)
    logging.info('Starting publisher')
    sender_process.start()
    await asyncio.sleep(0.5)
    sub = get_reader(subject=subject, deliver_policy='all')
    # await asyncio.gather(subsciber_task(sub), disconnector_task(messenger))
    await subsciber_task(sub)
    await sub.close()

    sender_process.join()
    logging.info('Sender joined')


@pytest.mark.skip(reason="For manual run, with NATS disconnect only")
@pytest.mark.nats
async def test_messenger_pub_sub_with_broken_nats(messenger, unique_subject, nats_server):

    subject = unique_subject

    now = datetime.datetime.now()

    async def subsciber_task(sub):
        logging.info('Start subscriber loop')
        async for data, meta in sub:
            logging.info(f'Received message: [{meta["receive_mode"]}] '
                         f'{data["n"]} {"(final)" if data["final"] else ""}')
            if data['final']:
                logging.info('It have been final message')
                break

    sender_process1 = multiprocessing.Process(
        target=publisher_process,
        kwargs=dict(host=nats_server['host'], port=nats_server['port'], subject=subject, n=10, final=False)
    )
    sender_process2 = multiprocessing.Process(
        target=publisher_process,
        kwargs=dict(host=nats_server['host'], port=nats_server['port'], subject=subject, n=5, final=False)
    )
    sender_process3 = multiprocessing.Process(
        target=publisher_process,
        kwargs=dict(host=nats_server['host'], port=nats_server['port'], subject=subject, n=5, final=True)
    )

    await messenger.purge(subject)
    logging.info('Purged')
    await asyncio.sleep(0.5)
    logging.info('Starting publisher1')
    sender_process1.start()
    sender_process1.join()
    logging.info('Finished Publisher1')
    await asyncio.sleep(0.5)
    sub = get_reader(subject=subject, deliver_policy='all')
    t = asyncio.create_task(subsciber_task(sub))
    # await subsciber_task(sub)
    await asyncio.sleep(0.5)
    seconds = 30
    logging.warning(f'Break for reconnect {seconds}s')
    await asyncio.sleep(seconds)
    logging.info('Starting publisher2')
    sender_process2.start()
    sender_process2.join()
    logging.info('Finished Publisher2')
    # sub._reconnect_needed.set()
    await asyncio.sleep(5)
    logging.info('Starting publisher3')
    sender_process3.start()
    sender_process3.join()
    logging.info('Finished Publisher3')
    await t
    await sub.close()

@pytest.mark.skip(reason="Long running for manual tests")
@pytest.mark.nats
async def test_messenger_pub_sub_long_run(messenger, unique_subject):
    subject = unique_subject

    async def subsciber_task(sub):
        await asyncio.sleep(2)
        logging.info('Start subscriber loop')
        async for data, meta in sub:
            logging.info(f'Received message {meta["nats"]["seq"]}: [{meta["receive_mode"]}] '
                         f'{data["n"]} {"(final)" if data["final"] else ""}')
            if data['final']:
                logging.info('It have been final message')
                break

    await messenger.purge(subject)
    logging.info('Purged')
    await asyncio.sleep(0.5)
    pub = get_publisher(subject=subject)
    sub = get_reader(subject=subject, deliver_policy='all')
    await asyncio.gather(
        publisher_task(pub, sleep_time=1, n=100),
        subsciber_task(sub)
    )
    await sub.close()



@pytest.mark.skip(reason="Always fail, after NATS.close, JS can not be reestablished")
@pytest.mark.nats
async def test_subscribe_after_reconnect(messenger, unique_subject):
    subject = unique_subject

    assert messenger.connection is not None
    assert messenger.connection.nc is not None
    assert messenger.connection.js is not None
    logging.info(f'Status1 : {messenger.connection.status}')

    sub1 = get_reader(subject=subject, deliver_policy='new')
    await messenger.connection.nc.close()
    await messenger.connection.update_statuses()
    logging.info(f'Status2 : {messenger.connection.status}')
    await messenger.connection.connect()
    await messenger.connection.nc.connect()
    logging.info(f'Status3 : {messenger.connection.status}')
    js = messenger.connection.nc.jetstream()
    assert js is not None
    stream = await js.find_stream_name_by_subject(subject)
    logging.info(f'Stream : {stream}')
    await sub1.close()

