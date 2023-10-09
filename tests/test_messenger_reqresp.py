import pytest

from serverish.base import MessengerRequestNoResponders, MessengerRequestJetStreamSubject
from serverish.messenger import request, get_rpcresponder, Messenger, Rpc
from tests.test_connection import ci
from tests.test_nats import is_nats_running


def cb(rpc: Rpc):
    data = rpc.data
    c = data['a'] + data['b']
    ret_data = {'c': c}
    rpc.set_response(data=ret_data)



@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_rpc_create_responder():

    async with Messenger().context(host='localhost', port=4222) as mess:
        async with get_rpcresponder('test.messenger.test_messenger_rpc_create_responder') as r:
            await r.register_function(cb)

@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_rpc_single_no_js():

    async with Messenger().context(host='localhost', port=4222) as mess:
        async with get_rpcresponder('test_no_js.messenger.test_messenger_rpc_create_responder') as r:
            await r.register_function(cb)
            data, meta = await request('test_no_js.messenger.test_messenger_rpc_create_responder', data={'a': 1, 'b': 2})
            assert data['c'] == 3

@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_rpc_single_js():

    async with Messenger().context(host='localhost', port=4222) as mess:
        try:
            async with get_rpcresponder('test.messenger.test_messenger_rpc_create_responder') as r:
                await r.register_function(cb)
                data, meta = await request('test.messenger.test_messenger_rpc_create_responder', data={'a': 1, 'b': 2})
                print (data, meta)
        except MessengerRequestJetStreamSubject:
            pass
        else:
            assert False, "Should have raised MessengerRequestJetStreamSubject"

@pytest.mark.asyncio  # This tells pytest this test is async
@pytest.mark.skipif(ci, reason="JetStreams Not working on CI")
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_messenger_rpc_single_noresponders():

    async with Messenger().context(host='localhost', port=4222) as mess:
        async with get_rpcresponder('test_no_js.messenger.test_messenger_rpc_create_responder') as r:
            try:
                data, meta = await request('test_no_js.messenger.test_messenger_rpc_create_responder', data={'a': 1, 'b': 2})
            except MessengerRequestNoResponders:
                pass
            else:
                assert False, "Should have raised MessengerRequestNoResponders"

            await r.register_function(cb)
            data, meta = await request('test_no_js.messenger.test_messenger_rpc_create_responder', data={'a': 1, 'b': 2})
            assert data['c'] == 3
