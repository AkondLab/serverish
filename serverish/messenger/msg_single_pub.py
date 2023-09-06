from __future__ import annotations

from serverish.messenger import Messenger
from serverish.messenger.msg_publisher import MsgPublisher


class MsgSinglePublisher(MsgPublisher):
    """A class for publishing single message to a subject

    Use this class if you want to publish single data to a messenger subject.
    There is no need to open/close publisher nor usage of context manager, it will be done automatically.
    Anyway Messenger should be initialized before usage of this class.
    """

    async def publish(self, data: dict | None = None, meta: dict | None = None, **kwargs) -> dict:
        await self.open()
        try:
            return await super().publish(data, meta=meta, **kwargs)
        finally:
            await self.close()


def get_singlepublisher(subject) -> MsgSinglePublisher:
    """Returns a single-publisher for a given subject

    Args:
        subject (str): subject to publish to

    Returns:
        MsgSinglePublisher: a publisher for the given subject

    """
    return Messenger.get_singlepublisher(subject)


async def single_publish(subject, data: dict | None = None, meta: dict | None = None, **kwargs) -> dict:
    """Publishes a single message to publisher subject

    Args:
        subject (str): subject to publish to
        data (dict): message data
        meta (dict): message metadata
        kwargs: additional arguments to pass to the connection

    """
    pub = get_singlepublisher(subject)
    return await pub.publish(data, meta=meta, **kwargs)
