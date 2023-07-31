"""
Communication module for serverish

Those are higher level classes and functions for JetStream connection.

Functions:
    get_publisher
    get_subscriber

"""
import json
import logging

import param

from serverish.collector import Collector
from serverish.connection_jets import ConnectionJetStream
from serverish.manageable import Manageable
from serverish.singleton import Singleton

log = logging.getLogger(__name__.rsplit('.')[-1])


class Messenger(Singleton):
    conn = param.ClassSelector(class_=ConnectionJetStream, default=None, allow_None=True, doc="Messenger Connection")

    def __init__(self, name: str = None, parent: Collector = None, **kwargs) -> None:
        super().__init__(name, parent, **kwargs)

    @property
    def connection(self) -> ConnectionJetStream:
        if self.conn is None:
            raise ValueError("Messenger Connection opened, use configure(host, port) first")
        return self.conn

    async def open(self, host, port):
        if self.conn is not None:
            log.warning("Messenger Connection already opened, ignoring")
        self.conn = ConnectionJetStream(host, port)
        await self.conn.connect()

    async def close(self):
        if self.conn is not None:
            await self.connection.disconnect()
            self.conn = None

    def encode(self, msg: dict) -> bytes:
        return json.dumps(msg).encode('utf-8')

    def decode(self, bdata: bytes) -> dict:
        return json.loads(bdata.decode('utf-8'))

    def get_publisher(topic):
        """Returns a publisher for a given topic

        Args:
            topic (str): topic to publish to

        Returns:
            MsgPublisher: message publisher
        """
        return MsgPublisher(topic=topic, parent=Messenger())



class MsgTopic(Manageable):
    topic: str = param.String(default=None, allow_None=True, doc="User topic to publish to, prefix may be added")
    """Message topic operator

    Message publisher/subsriber etc base

    Args:
        topic (str): topic to publish to
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    @property
    def messenger(self) -> Messenger | None:
        if isinstance(self.parent, Messenger):
            return self.parent
        return None

    @property
    def connection(self) -> ConnectionJetStream:
        return self.messenger.connection


    async def open(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def __aenter__(self) -> None:
        await self.open()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()


class MsgPublisher(MsgTopic):
    async def publish(self, data: dict = None, **kwargs) -> None:
        """Publishes a message

        Args:
            data (dict): message data
            kwargs: additional arguments to pass to the connection

        """
        bdata = self.messenger.encode(data)
        await self.connection.js.publish(self.topic, bdata, **kwargs)


async def get_publisher(topic):
    """Returns a publisher for a given topic

    Args:
        topic (str): topic to publish to

    Returns:
        Publisher: a publisher for the given topic

    """
    return Messenger.get_publisher(topic)



