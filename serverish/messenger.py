"""
Communication module for serverish

Those are higher level classes and functions for JetStream connection.

Functions:
    get_publisher
    get_subscriber

"""
from __future__ import annotations

import logging
import json
import time

import jsonschema
import param

from serverish.collector import Collector
from serverish.connection_jets import ConnectionJetStream
from serverish.idmanger import gen_id
from serverish.manageable import Manageable
from serverish.msgvalidator import MsgValidator
from serverish.singleton import Singleton

log = logging.getLogger(__name__.rsplit('.')[-1])


class Messenger(Singleton):
    conn = param.ClassSelector(class_=ConnectionJetStream, default=None, allow_None=True, doc="Messenger Connection")
    validation = param.Boolean(default=True, doc="Validate messages against schema")

    def __init__(self, name: str = None, parent: Collector = None, **kwargs) -> None:
        self.validator = MsgValidator()
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

    @staticmethod
    def create_meta(meta: dict | None = None) -> dict:
        """Creates meta data for a message

        Args:
            meta (dict): meta data to be sent, many metadata will be added automatically

        Returns:
            dict: meta data
        """
        ret = {
            'id': gen_id('msg'),
            # "sender": "sender_name",
            # "receiver": "receiver_name",  # only for direct messages
            'ts': list(time.gmtime()),
            'trace_level': logging.DEBUG,  # Message trace will be logged if loglevel <= trace_level
            "message_type": "",
            'tags': [],
        }
        if meta is not None:
            ret.update(meta)
        return ret

    @classmethod
    def create_msg(cls, data: dict | None = None, meta: dict | None = None) -> dict:
        """Creates a message with data and meta

        Args:
            data (dict): data to be sent
            meta (dict): meta data to be sent, many metadata will be added automatically

        Returns:
            dict: message
        """
        msg = {}
        if data is not None:
            msg['data'] = data
        msg['meta'] = cls.create_meta(meta)
        return msg

    @staticmethod
    def split_msg(msg: dict) -> tuple[dict, dict]:
        """Splits message into data and meta

        Args:
            msg (dict): message

        Returns:
            tuple[dict, dict]: data, meta
        """
        data = msg.get('data', {})
        meta = msg.get('meta', {})
        return data, meta

    @classmethod
    def msg_to_repr(cls, msg: dict) -> str:
        """Converts message to a string representation

        Args:
            msg (dict): message

        Returns:
            str: string representation
        """
        data, meta = cls.split_msg(msg)
        ts = time.strftime("%Y-%m-%dT%H:%M:%S", meta.pop('ts'))
        id = meta.pop('id')
        smeta = ' '.join(f"{k}:{v}" for k, v in meta.items())
        sdata = json.dumps(data)
        return f"{ts} {id} {smeta} data:{sdata[:100]}"

    @classmethod
    def log_msg_trace(cls, msg: dict, comment: str) -> None:
        """Logs a message if trace_level is high enough

        Args:
            msg (dict): message
            comment (str): comment to log
        """
        data, meta = cls.split_msg(msg)
        trace_level = meta.get('trace_level', logging.DEBUG)
        if trace_level < log.getEffectiveLevel():
            return
        log.log(trace_level, f"{comment} [{cls.msg_to_repr(msg)}]")

    def msg_validate(self, msg: dict):
        """Validates message, raises jsonschema.ValidationError if invalid

        Args:
            msg (dict): message
        """
        if self.validation:
            self.validator.validate(msg)

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
    async def publish(self, data: dict | None = None, meta: dict | None = None, **kwargs) -> dict:
        """Publishes a message

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
        self.messenger.log_msg_trace(msg, f"PUB to {self.topic}")
        await self.connection.js.publish(self.topic, bdata, **kwargs)
        return msg


async def get_publisher(topic):
    """Returns a publisher for a given topic

    Args:
        topic (str): topic to publish to

    Returns:
        Publisher: a publisher for the given topic

    """
    return Messenger.get_publisher(topic)



