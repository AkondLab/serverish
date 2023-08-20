"""Tests NATS JetStream diagnostics functionality"""

import asyncio

from serverish.connection_jets import ConnectionJetStream


def print_status(d):
    for t, s in d.items():
        print(f'{t:15}: {str(s):4}  ({s.msg})')

async def diagnose_stream_config_with_diagnose():
    c = ConnectionJetStream(host='localhost', port=4222)
    c.declared_subjects.append('srvh.test')
    print('Diagnose:')
    async with c:
        codes = await c.diagnose(no_deduce=True)
        print_status(codes)


async def diagnose_stream_config_with_statuses():
    c = ConnectionJetStream(host='localhost', port=4222)
    c.declared_subjects.append('srvh.test')
    try:
        print('Statuses:')
        async with c:
            print_status(c.status)
    except Exception as e:
        print(f'Exception: {e}')
        print_status(c.status)

async def diagnose_stream_config_in_loop():
    c = ConnectionJetStream(host='localhost', port=4222)
    c.declared_subjects.append('srvh.test')
    async with c:
        while True:
            try:
                print('Statuses:')
                print_status(c.status)
            except Exception as e:
                print(f'Exception in statuses: {e}')
                print_status(c.status)
            await asyncio.sleep(2)
            try:
                print('Diagnose:')
                codes = await c.diagnose(no_deduce=True)
                print_status(codes)
            except Exception as e:
                print(f'Exception in diagnose: {e}')
                print_status(c.status)
            await asyncio.sleep(2)


if __name__ == '__main__':
    print('Statuses after connecting')
    asyncio.run(diagnose_stream_config_with_statuses())
    print('Daignose result')
    asyncio.run(diagnose_stream_config_with_diagnose())
    print('Continuous checking of statuses (infinite loop Ctrl+C to stop)')
    asyncio.run(diagnose_stream_config_in_loop())
