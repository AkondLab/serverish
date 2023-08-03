import asyncio

import nats
import nats.errors
import pytest

from tests.test_nats import is_nats_running


# @pytest.mark.asyncio
# @pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
# async def test_js_strem_wrong_name():
#     nc = await nats.connect("localhost")
#     js = nc.jetstream()
#     try:
#         await js.add_stream(name="stream.with.wrong.name")
#     except nats.errors.Error as e:
#         assert not isinstance(e, nats.errors.TimeoutError) , \
#             "TimeoutError should not be raised, see https://github.com/nats-io/nats.py/issues/471"
#     else:
#         pytest.fail("Error should have been raised")
#     finally:
#         await nc.close()

@pytest.mark.asyncio
@pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
async def test_js_strem_good_name():
    nc = await nats.connect("localhost")
    js = nc.jetstream()
    try:
        await js.add_stream(name="test-str2", subjects=["fooxxx"])
    except nats.errors.Error as e:
        pytest.fail(f"Error should not have been raised on stream creation {e}")
    finally:
        await nc.close()

# @pytest.mark.asyncio
# @pytest.mark.skipif(not is_nats_running(), reason="requires nats server on localhost:4222")
# async def test_js_many_pub():
#     nc = await nats.connect("localhost")
#     js = nc.jetstream()
#     tasks = []
#     for i in range(100000):
#         task = asyncio.create_task(js.publish("foo", f"hello world: {i}".encode(), timeout=500))
#         tasks.append(task)
#     responses = await asyncio.gather(*tasks)