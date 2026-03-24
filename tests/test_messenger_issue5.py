import nats
import pytest

from serverish.messenger import Messenger, get_reader


@pytest.mark.nats
async def test_messenger_issue5_subject_not_in_stream(messenger):
    import uuid
    # Use a subject guaranteed to NOT be in any stream (outside test.> wildcard)
    orphan_subject = f'nostream.issue5.{uuid.uuid4().hex[:8]}'
    reader = get_reader(orphan_subject,
                        deliver_policy="last")
    reader.error_behavior = "RAISE"
    try:
        cfg = await reader.read_next()
    except nats.js.errors.NotFoundError:
        pass
    else:
        assert False, 'Should raise NotFoundError'
