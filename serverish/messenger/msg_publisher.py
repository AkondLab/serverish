from __future__ import annotations

import time

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

    Health monitoring:
        The publisher tracks publish statistics accessible via `health_status` property:
        - publish_count: Total successful publishes
        - error_count: Total publish errors
        - last_publish_time: Timestamp of last successful publish
        - last_error: Last error encountered
    """

    raise_on_publish_error = param.Boolean(default=True, doc="Raise on publish error")

    def __init__(self, **kwargs) -> None:
        # Health monitoring fields
        self._publish_count: int = 0
        self._error_count: int = 0
        self._last_publish_time: float | None = None
        self._last_error: Exception | None = None
        super().__init__(**kwargs)

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
            # Track successful publish
            self._publish_count += 1
            self._last_publish_time = time.monotonic()
        except AttributeError as e:  # no js - not connected
            self._error_count += 1
            self._last_error = e
            log.error(f"Trying to publish to subject '{self.subject}' failed. JetStream not connected")
            raise MessengerNotConnected(f"Trying to publish to subject '{self.subject}' failed. JetStream not connected")
        except (nats.errors.NoRespondersError, nats.js.errors.NoStreamResponseError):
            # it's OK for non-jetstream, we just don't have subscribers yet
            # Still count as successful publish (message was sent, just no stream to persist)
            self._publish_count += 1
            self._last_publish_time = time.monotonic()
            log.debug(
                f'No subscribers yet returned by NATS server for subject {self.subject}, '
                f'if it was meant to be jetstream, the subject does not exist in any stream!')
        except Exception as e:
            self._error_count += 1
            self._last_error = e
            log.error(f"Trying to publish to subject '{self.subject}' failed. "
                      f"Message {msg['meta']['id']} publish error: {e}")
            if self.raise_on_publish_error:
                raise e
            else:
                msg['meta']['tags'].append('error')
                msg['meta']['status'] = str(e)
        return msg

    @property
    def health_status(self) -> dict:
        """Returns current health status of the publisher for monitoring

        Returns:
            dict with health information:
                - is_open: Whether the publisher is currently open
                - subject: The subject being published to
                - publish_count: Total successful publishes
                - error_count: Total publish errors
                - last_publish_time: Timestamp of last successful publish (monotonic time)
                - last_publish_ago: Seconds since last publish or None
                - last_error: String representation of last error or None
        """
        last_publish_ago = None
        if self._last_publish_time is not None:
            last_publish_ago = time.monotonic() - self._last_publish_time

        return {
            'is_open': self.is_open,
            'subject': self.subject,
            'publish_count': self._publish_count,
            'error_count': self._error_count,
            'last_publish_time': self._last_publish_time,
            'last_publish_ago': last_publish_ago,
            'last_error': str(self._last_error) if self._last_error else None,
        }


def get_publisher(subject) -> MsgPublisher:
    """Returns a publisher for a given subject

    Args:
        subject (str): subject to publish to

    Returns:
        Publisher: a publisher for the given subject

    """
    return Messenger.get_publisher(subject)
