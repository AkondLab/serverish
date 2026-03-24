import nats
import pytest

from serverish.messenger import Messenger, get_reader


@pytest.mark.nats
async def test_messenger_issue5_subject_not_in_stream(messenger, unique_subject):

    reader = get_reader(unique_subject,
                        deliver_policy="last")
    reader.error_behavior = "RAISE"
    try:
        cfg = await reader.read_next()
    except nats.js.errors.NotFoundError:
        pass
    else:
        assert False, 'Shoud raise NotFoundError'
