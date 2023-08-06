"""
Communication module for serverish

Those are higher level classes and functions for JetStream connection.

Functions:
    get_publisher
    get_subscriber

"""
from __future__ import annotations

import contextlib
import logging
import json
import time
from asyncio import Event

import jsonschema
import param

from nats.aio.subscription import Subscription
from nats.js.api import DeliverPolicy, ConsumerConfig
import nats.errors
import nats.js.errors

from serverish.collector import Collector
from serverish.connection_jets import ConnectionJetStream
from serverish.idmanger import gen_id
from serverish.manageable import Manageable
from serverish.msgvalidator import MsgValidator
from serverish.singleton import Singleton

log = logging.getLogger(__name__.rsplit('.')[-1])


class MsgData():
    def __dir__(self) -> dict:
        return self.__dict__

class Messenger(Singleton):
    conn = param.ClassSelector(class_=ConnectionJetStream, default=None, allow_None=True, doc="Messenger Connection")
    validation = param.Boolean(default=True, doc="Validate messages against schema")
    default_host = param.String(default='localhost', doc="Default NATS host name")
    default_port = param.Integer(default=4222, doc="Default NATS port number")

    def __init__(self, name: str = None, parent: Collector = None, **kwargs) -> None:
        self.validator = MsgValidator()
        super().__init__(name, parent, **kwargs)

    @property
    def connection(self) -> ConnectionJetStream:
        if self.conn is None:
            raise ValueError("Messenger Connection opened, use configure(host, port) first")
        return self.conn

    async def open(self, host: str | None = None, port: int | None = None):
        if host is None:
            host = self.default_host
        if port is None:
            port = self.default_port
        if self.conn is not None:
            log.warning("Messenger Connection already opened, ignoring")
        self.conn = ConnectionJetStream(host, port)
        await self.conn.connect()

    async def close(self):
        if self.conn is not None:
            await self.connection.disconnect()
            self.conn = None

    @property
    def is_open(self) -> bool:
        return self.conn is not None

    @contextlib.asynccontextmanager
    async def context(self, host: str | None, port: int | None):
        """Context manager for connection

        Args:
            host (str): Hostname or IP address
            port (int): Port number

        Returns:
            Messenger: self

        Usage:
            msg = Messenger()
            async with msg.context(host, port) as msg:
                pass # do something
        """
        await self.open(host, port)
        try:
            yield self
        finally:
            await self.close()


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
        ts = time.strftime("%Y-%m-%dT%H:%M:%S", tuple(meta.pop('ts')))
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

    @staticmethod
    def get_publisher(subject: str) -> MsgPublisher:
        """Returns a publisher for a given subject

        Args:
            subject (str): subject to publish to

        Returns:
            MsgPublisher: message publisher
        """
        return MsgPublisher(subject=subject, parent=Messenger())

    @staticmethod
    def get_reader(subject: str, queue: str | None = None, durable_name: str | None = None, **kwargs) -> MsgReader:
        """Returns a reader for a given subject

        Args:
            subject (str): subject to subscribe to
            queue (str): queue name, if None, subject is used
            durable_name (str): durable name, if None, queue is used

        Returns:
            MsgReader: message subscriber

        Usage:
            reader = Messenger.get_reader('subject'):
            async for msg in reader:
                print(msg)
        """
        return MsgReader(subject=subject, queue=queue, durable_name=durable_name, parent=Messenger(), **kwargs)





class MsgDriver(Manageable):
    subject: str = param.String(default=None, allow_None=True, doc="User subject to publish to, prefix may be added")
    """Message subject operator

    Message publisher/subsriber etc base

    Args:
        subject (str): subject to publish to
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


class MsgPublisher(MsgDriver):
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
        self.messenger.log_msg_trace(msg, f"PUB to {self.subject}")
        try:
            await self.connection.js.publish(self.subject, bdata, **kwargs)
        except nats.js.errors.NoStreamResponseError as e:
            log.error(f"Trying to publish to subject '{self.subject}' which may be not in stream: "
                      f"Message {msg['meta']['id']} publish error: {e}")
            raise e
        except Exception as e:
            log.error(f"Trying to publish to subject '{self.subject}' failed. "
                      f"Message {msg['meta']['id']} publish error: {e}")
            raise e
        return msg

class MsgReader(MsgDriver):
    queue: str = param.String(default=None, allow_None=True, doc="Queue name, if None, subject is used")
    durable_name: str = param.String(default=None, allow_None=True, doc="Durable name, if None, queue is used")
    deliver_policy: str = param.ObjectSelector(default='all',
                                               objects=['all', 'last', 'new',
                                                        'by_start_sequence',
                                                        'by_start_time',
                                                        'last_per_subject'
                                                        ],
                                               doc="Delivery policy, for underlying JetStream subscription")
    opt_start_time = param.Date(default=None, allow_None=True,
                                                          doc="Start time, for underlying JetStream subscription")
    opt_start_seq: int | None = param.Integer(default=None, allow_None=True,
                                              doc="Start sequence, for underlying JetStream subscription")

    def __init__(self, **kwargs) -> None:
        self.subscription: Subscription | None = None
        self._stop: Event = Event()
        super().__init__(**kwargs)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.subscription is None:
            await self.open()

        while True:
            if self._stop.is_set():
                await self.close()
                raise StopAsyncIteration
            try:
                bmsg = await self.subscription.next_msg()
                break
            except nats.errors.TimeoutError:
                pass  # keep iterating  # TODO: Consider transformation for push based when stream gets empty
            except Exception as e:
                log.error(f"During interation on {self}, subscription.next_msg raised {e}")
                await self.close()
                raise e

        msg = self.messenger.decode(bmsg.data)
        self.messenger.log_msg_trace(msg, f"SUB iteration from {self.subject}")
        data, meta = self.messenger.split_msg(msg)
        return data, meta

    async def open(self) -> None:
        if self.subscription is not None:
            raise RuntimeError("Subscription already open, do not reuse MsgSubscription instances")

        js = self.connection.js

        # Convert the delivery policy from a string to the appropriate DeliverPolicy enum
        deliver_policy_enum = DeliverPolicy(self.deliver_policy)

        # from_time handling:
        if self.opt_start_time is None:
            opt_start_time = None
        else:
            opt_start_time = self.opt_start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        # Create the consumer configuration
        consumer_conf = ConsumerConfig(
            deliver_policy=deliver_policy_enum,
            opt_start_time=opt_start_time,
            opt_start_seq=self.opt_start_seq,
            durable_name=self.durable_name,
        )

        # Return a new Subscription
        self.subscription = await js.subscribe(self.subject, queue=self.queue, config=consumer_conf)

    async def close(self) -> None:
        if self.subscription is not None:
            await self.subscription.unsubscribe()
            self.subscription = None
        else:
            raise RuntimeError("Subscription already closed")

    async def drain(self, timeout: float = 0.0) -> None:
        """Drains the subscription, returns when no more messages are available

        Should not be called from within an iteration (use stop instead)

        Args:
            timeout (float): timeout in seconds

        """
        if self.subscription is None:
            raise RuntimeError("Subscription not open")

        await self.subscription.drain()
        self.stop()

    def stop(self) -> None:
        """Stops the subscription, returns immediately

        """
        if self.subscription is None:
            raise RuntimeError("Subscription not open")

        self._stop.set()



async def get_publisher(subject) -> MsgPublisher:
    """Returns a publisher for a given subject

    Args:
        subject (str): subject to publish to

    Returns:
        Publisher: a publisher for the given subject

    """
    return Messenger.get_publisher(subject)


async def get_reader(subject, queue=None, durable_name=None, **kwargs) -> MsgReader:
    """Returns a subscription for a given subject, manages single subscription

    Args:
        subject (str): subject to subscribe to
        queue (str): queue name, if None, subject is used
        durable_name (str): durable name, if None, queue is used
        kwargs: additional arguments to pass to the connection

    Returns:
        Subscriber: a reader for the given subject

    Usage:
        async for msg in get_reader("subject"):
            print(msg)

    """
    return Messenger.get_reader(subject, queue=queue, durable_name=durable_name, **kwargs)

