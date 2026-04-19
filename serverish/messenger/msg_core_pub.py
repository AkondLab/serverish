from __future__ import annotations

import jsonschema
import logging

import param

from serverish.base import MessengerNotConnected
from serverish.messenger.messenger import MsgDriver, log


class MsgCorePub(MsgDriver):
    """A class for publishing data to a subject using core NATS (no JetStream)

    Use this class if you want to publish fire-and-forget messages over core NATS,
    on subjects that are not backed by a JetStream stream.

    Parameters:
        raise_on_publish_error (bool): Raise on publish error, default ``True`` re-raises underlying exceptions
    """

    raise_on_publish_error = param.Boolean(default=True, doc="Raise on publish error")

    async def publish(self, data: dict | None = None, meta: dict | None = None) -> dict:
        """Publishes a message to the subject using core NATS (no JetStream)

        Args:
            data (dict): message data
            meta (dict): message metadata

        Returns:
            dict: published message, or message with the ``error`` tag in meta if failed

        Raises:
            MessengerNotConnected: if the NATS connection is not available
            jsonschema.ValidationError: if the message fails schema validation
        """
        from serverish.messenger import Messenger
        messenger: Messenger = self.messenger
        msg = messenger.create_msg(data, meta)
        bdata = messenger.encode(msg)
        try:
            messenger.msg_validate(msg)
        except jsonschema.ValidationError as e:
            log.error(f"Message {msg['meta']['id']} validation error: {e}")
            raise e
        messenger.log_msg_trace(msg.get('data', {}), msg['meta'], f"CORE PUB to {self.subject}")
        try:
            nc = self.connection.nc
            if nc is None:
                raise MessengerNotConnected(
                    f"Trying to publish to subject '{self.subject}' failed. NATS not connected"
                )
            await nc.publish(self.subject, bdata)
        except MessengerNotConnected:
            raise
        except AttributeError:
            raise MessengerNotConnected(
                f"Trying to publish to subject '{self.subject}' failed. NATS not connected"
            )
        except Exception as e:
            log.error(
                f"Trying to publish to subject '{self.subject}' failed. "
                f"Message {msg['meta']['id']} publish error: {e}"
            )
            if self.raise_on_publish_error:
                raise e
            else:
                msg['meta']['tags'].append('error')
                msg['meta']['status'] = str(e)
        return msg


def get_corepublisher(subject: str) -> 'MsgCorePub':
    """Returns a core NATS publisher for a given subject (no JetStream)

    Use this for fire-and-forget subjects that are not backed by a JetStream stream.

    Args:
        subject (str): subject to publish to

    Returns:
        MsgCorePub: a core-NATS publisher for the given subject

    Usage::

        async with Messenger().context(host, port):
            pub = get_corepublisher("svc.command.site.tts.say")
            await pub.open()
            await pub.publish(data={"text": "hello"})
            await pub.close()
    """
    from serverish.messenger import Messenger
    return Messenger.get_corepublisher(subject=subject)
