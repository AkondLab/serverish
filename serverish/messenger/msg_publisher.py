from __future__ import annotations

import jsonschema
import nats.errors
import nats.js

from serverish.messenger import Messenger
from serverish.messenger.messenger import MsgDriver, log


class MsgPublisher(MsgDriver):
    """A class for publishing data to a Messenger subject

    Use this class if you want to publish data to a messenger subject.
    Check for specialist publishers for common use cases.
    """
    async def publish(self, data: dict | None = None, meta: dict | None = None, **kwargs) -> dict:
        """Publishes a messages to publisher subject

        Args:
            data (dict): message data
            kwargs: additional arguments to pass to the connection

        """
        msg = self.messenger.create_msg(data, meta)
        bdata = self.messenger.encode(msg)
        try:
            self.messenger.msg_validate(msg)
        except jsonschema.ValidationError as e:
            log.error(f"Message {msg['meta']['id']} validation error: {e}")
            raise e
        self.messenger.log_msg_trace(msg, f"PUB to {self.subject}")
        try:
            await self.connection.js.publish(self.subject, bdata, **kwargs)
        except (nats.errors.NoRespondersError, nats.js.errors.NoStreamResponseError):
            # it's OK, we just don't have subscribers yet
            pass
        except Exception as e:
            log.error(f"Trying to publish to subject '{self.subject}' failed. "
                      f"Message {msg['meta']['id']} publish error: {e}")
            raise e
        return msg


def get_publisher(subject) -> MsgPublisher:
    """Returns a publisher for a given subject

    Args:
        subject (str): subject to publish to

    Returns:
        Publisher: a publisher for the given subject

    """
    return Messenger.get_publisher(subject)
