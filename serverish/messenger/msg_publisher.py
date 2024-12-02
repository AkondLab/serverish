from __future__ import annotations

import jsonschema
import nats.errors
import nats.js
import param

from serverish.base import MessengerNotConnected
from serverish.messenger import Messenger
from serverish.messenger.messenger import MsgDriver, log


class MsgPublisher(MsgDriver):
    """A class for publishing data to a Messenger subject

    Use this class if you want to publish data to a messenger subject.
    Check for specialist publishers for common use cases.

    Parameters:
        raise_on_publish_error (bool): Raise on publish error, default `True` re-raises underlying exceptions
    """

    raise_on_publish_error = param.Boolean(default=True, doc="Raise on publish error")

    async def publish(self, data: dict | None = None, meta: dict | None = None, **kwargs) -> dict:
        """Publishes a messages to publisher subject

        Args:
            data (dict): message data
            meta (dict): message metadata
            kwargs: additional arguments to pass to the connection
        Returns:
            dict: published message, or message to be published with the `error` tag in meta if failed
        Raises:
            Raises nats errors if the message could not be published until `raise_on_publish_error` is set to True
            otherwise logs the error, and returns the message with the `error` tag and `status` set to the error message.
        """
        msg = self.messenger.create_msg(data, meta)
        bdata = self.messenger.encode(msg)
        try:
            self.messenger.msg_validate(msg)
        except jsonschema.ValidationError as e:
            log.error(f"Message {msg['meta']['id']} validation error: {e}")
            raise e
        self.messenger.log_msg_trace(msg['data'], msg['meta'], f"PUB to {self.subject}")
        try:
            await self.connection.js.publish(self.subject, bdata, **kwargs)
        except AttributeError: # no js - not connected
            log.error(f"Trying to publish to subject '{self.subject}' failed. JestStream not connected")
            raise MessengerNotConnected(f"Trying to publish to subject '{self.subject}' failed. JestStream not connected")
        except (nats.errors.NoRespondersError, nats.js.errors.NoStreamResponseError):
            # it's OK for non-jetstream, we just don't have subscribers yet
            log.debug(
                f'No subscribers yet returned by NATS server for subject {self.subject}, '
                f'if it was ment to be jetstream, the subject does not exist in any stream!')
            pass
        except Exception as e:
            log.error(f"Trying to publish to subject '{self.subject}' failed. "
                      f"Message {msg['meta']['id']} publish error: {e}")
            if self.raise_on_publish_error:
                raise e
            else:
                msg['meta']['tags'].append('error')
                msg['meta']['status'] = str(e)
        return msg


def get_publisher(subject) -> MsgPublisher:
    """Returns a publisher for a given subject

    Args:
        subject (str): subject to publish to

    Returns:
        Publisher: a publisher for the given subject

    """
    return Messenger.get_publisher(subject)
