import nats
import pytest

from serverish.base.exceptions import MessengerReaderConfigError
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
    except MessengerReaderConfigError:
        # "No stream found" is a fatal configuration error — it is surfaced as
        # MessengerReaderConfigError (wrapping the original NotFoundError) so
        # that callers get a clear, actionable exception instead of a raw NATS
        # 404 that would otherwise retry forever in WAIT mode.
        pass
    else:
        assert False, 'Should raise MessengerReaderConfigError'
