import pytest

from serverish.base import MessengerRequestNoResponders, MessengerRequestJetStreamSubject
from serverish.messenger import request, get_rpcresponder, Messenger, Rpc


def cb(rpc: Rpc):
    data = rpc.data
    c = data['a'] + data['b']
    ret_data = {'c': c}
    rpc.set_response(data=ret_data)



@pytest.mark.nats
async def test_messenger_rpc_create_responder(messenger, unique_subject):

    async with get_rpcresponder(unique_subject) as r:
        await r.register_function(cb)

@pytest.mark.nats
async def test_messenger_rpc_single_js_error(messenger, unique_subject):
    # Use a JetStream subject prefix to trigger the error
    subject = f'test.{unique_subject}'

    try:
        async with get_rpcresponder(subject) as r:
            await r.register_function(cb)
            data, meta = await request(subject, data={'a': 1, 'b': 2})
            print (data, meta)
    except MessengerRequestJetStreamSubject:
        pass
    else:
        assert False, "Should have raised MessengerRequestJetStreamSubject"

@pytest.mark.nats
async def test_messenger_rpc_single_noresponders(messenger, unique_subject):
    # RPC uses core NATS, not JetStream. Use a non-JS subject prefix.
    subject = f'test_no_js.{unique_subject}'

    async with get_rpcresponder(subject) as r:
        try:
            data, meta = await request(subject, data={'a': 1, 'b': 2})
        except MessengerRequestNoResponders:
            pass
        else:
            assert False, "Should have raised MessengerRequestNoResponders"

        await r.register_function(cb)
        data, meta = await request(subject, data={'a': 1, 'b': 2})
        assert data['c'] == 3
