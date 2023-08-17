"""Tests NATS JetStream diagnostics functionality"""

import asyncio

from serverish.connection_jets import ConnectionJetStream


async def diagnose_stream_config():
    c = ConnectionJetStream(host='localhost', port=4222)
    c.declared_subjects.append('srvh.test')
    async with c:
        codes = await c.diagnose(no_deduce=True)
        for t, s in codes.items():
            print(f'{t:15}: {str(s):4}  ({s.msg})')


if __name__ == '__main__':
    asyncio.run(diagnose_stream_config())
