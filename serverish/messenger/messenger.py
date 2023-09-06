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
from typing import TYPE_CHECKING

import param

from serverish.base.collector import Collector
from serverish.connection.connection_jets import ConnectionJetStream
from serverish.base.idmanger import gen_id
from serverish.base.manageable import Manageable
from serverish.messenger.msgvalidator import MsgValidator
from serverish.base.singleton import Singleton

if TYPE_CHECKING:
    from serverish.messenger.msg_publisher import MsgPublisher
    from serverish.messenger.msg_reader import MsgReader


log = logging.getLogger(__name__.rsplit('.')[-1])


class MsgData:
    def __dir__(self) -> dict:
        return self.__dict__


class Messenger(Singleton):
    conn = param.ClassSelector(class_=ConnectionJetStream, default=None, allow_None=True, doc="Messenger Connection")
    validation = param.Boolean(default=True, doc="Validate messages against schema")
    default_host = param.List(default=['localhost'], item_type=str, doc="Default NATS host name(s)")
    default_port = param.List(default=[4222], item_type=int, doc="Default NATS port number(s)")

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
    def split_msg(msg: dict) -> tuple[dict, dict | None]:
        """Splits message into data and meta

        Args:
            msg (dict): message

        Returns:
            tuple[dict, dict]: data, meta
        """
        data = msg.get('data', {})
        meta = msg.get('meta', None)
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
        id_ = meta.pop('id')
        smeta = ' '.join(f"{k}:{v}" for k, v in meta.items())
        sdata = json.dumps(data)
        return f"{ts} {id_} {smeta} data:{sdata[:100]}"

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

    async def purge(self, subject: str) -> None:
        js = self.connection.js
        stream = await js.find_stream_name_by_subject(subject)
        await js.purge_stream(stream, subject=subject)


    @staticmethod
    def get_publisher(subject: str) -> "MsgPublisher":
        """Returns a publisher for a given subject

        Args:
            subject (str): subject to publish to

        Returns:
            MsgPublisher: message publisher
        """
        from serverish.messenger.msg_publisher import MsgPublisher
        return MsgPublisher(subject=subject, parent=Messenger())

    @staticmethod
    def get_reader(subject: str,
                   deliver_policy='all',
                   opt_start_time=None,
                   **kwargs) -> 'MsgReader':
        """Returns a reader for a given subject

        Args:
            subject (str): subject to subscribe to
            deliver_policy (str): deliver policy, one of 'all', 'last', 'new', 'by_start_time', will be passed to consumer config
            opt_start_time (datetime): start time for 'by_start_time' deliver policy, will be passed to consumer config
            kwargs: additional arguments to pass to the reader and underlying NATS consumer config

        Returns:
            MsgReader: message subscriber

        Usage:
            reader = Messenger.get_reader('subject'):
            async for msg in reader:
                print(msg)
        """
        from serverish.messenger.msg_reader import MsgReader
        return MsgReader(subject=subject,
                         parent=Messenger(),
                         deliver_policy=deliver_policy,
                         opt_start_time=opt_start_time,
                         consumer_cfg=kwargs)

    @staticmethod
    def get_singlepublisher(subject):
        """Returns a signle-publisher for a given subject

        Args:
            subject (str): subject to publish to

        Returns:
            MsgSinglePublisher: a publisher for the given subject

        """
        from serverish.messenger.msg_single_pub import MsgSinglePublisher
        return MsgSinglePublisher(subject=subject, parent=Messenger())

    @staticmethod
    def get_singlereader(subject,
                         deliver_policy='last',
                         **kwargs):
        from serverish.messenger.msg_single_read import MsgSingleReader
        return MsgSingleReader(subject=subject,
                               parent=Messenger(),
                               deliver_policy=deliver_policy,
                               **kwargs)

    @staticmethod
    def get_callbacksubscriber(subject,
                               deliver_policy='last',
                               **kwargs):
        from serverish.messenger.msg_callback_sub import MsgCallbackSubscriber
        return MsgCallbackSubscriber(subject=subject,
                                     parent=Messenger(),
                                     deliver_policy=deliver_policy,
                                     **kwargs)

    @staticmethod
    def get_rpcrequester(subject) -> 'MsgRpcRequester':
        """Returns a RPC requester for a given subject

        Args:
            subject (str): subject to publish to

        Returns:
            MsgRpcRequester: a publisher for the given subject

        """

        from serverish.messenger.msg_rpc_req import MsgRpcRequester
        return MsgRpcRequester(subject=subject,
                               parent=Messenger())


    @staticmethod
    def get_rpcresponder(subject) -> 'MsgRpcResponder':
        """Returns a callback-based subscriber RPC responder

        Args:
            subject (str): subject to read from

        Returns:
            MsgRpcResponder: a single-value reader for the given subject


        Usage:
            def callback(rpc: Rpc):
                c = rpc.data['a'] + rpc.data['b']
                rpc.set_response(data={'c': c})

            responder = MsgRpcResponder(subject='subject')
            responder.open()
            try:
                await responder.register_function(callback)
                # ... wait for incoming messages
            finally:
                await responder.close()
        """

        from serverish.messenger.msg_rpc_resp import MsgRpcResponder
        return MsgRpcResponder(subject=subject,
                               parent=Messenger())

    @staticmethod
    def get_progresspublisher(subject) -> 'MsgProgressPublisher':
        """Returns a progress tracking publisher for a given subject

        Args:
            subject (str): subject to report progress to

        Returns:
            MsgProgressPublisher: a publisher for the given subject

        """

        from serverish.messenger.msg_progress_pub import MsgProgressPublisher
        return MsgProgressPublisher(subject=subject,
                                    parent=Messenger())



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

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
