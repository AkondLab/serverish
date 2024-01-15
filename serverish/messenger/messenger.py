"""
Communication module for serverish

Those are higher level classes and functions for JetStream connection.

Functions:
    get_publisher
    get_subscriber

"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import json
import functools
from typing import TYPE_CHECKING

import param
import nats
from nats.aio.msg import Msg

from serverish.base import dt_utcnow_array, dt_from_array, Task, create_task, dt_ensure_array
from serverish.base.collector import Collector
from serverish.connection.connection_jets import ConnectionJetStream
from serverish.base.idmanger import gen_id
from serverish.base.manageable import Manageable
from serverish.messenger.msgvalidator import MsgValidator
from serverish.base.singleton import Singleton
from serverish.base import MessengerCannotConnect

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
        self.opener_task: Task | None = None
        super().__init__(name, parent, **kwargs)

    @property
    def connection(self) -> ConnectionJetStream:
        if self.conn is None:
            raise ValueError("Messenger connection have not been opened, use configure(host, port) first")
        return self.conn

    async def open(self, host: str | None = None, port: int | None = None,
                   wait: float | bool = True, timeout: float | None = None) -> Task | None:
        """Opens a connection to NATS

        Should be called before any other `Messenger` method, directly or via context manager:
            async with Messenger().context(host, port) as msg:
                await msg.publish(...)
                # and do your stuff utilizing the messenger

        If the connection can not be established, this method will keep trying to connect until `timeout`,
        if `timeout == None` (default), it will keep trying forever.

        If `wait` is a number, if the connection can not be established after `wait` seconds, method will return
        and the connection will be established in the background (`timeout` still applies).
        Note, that `wait == False is equivalent to `wait == 0`, and will return immediately, without waiting for the
        connection to be established.
        Also note, that if `wait` is a number or `False`, the method will return without ensuring that the connection
        is established, and the caller will have to check `Messenger.is_open` or wait for the returned task to finish.

        Args:
            host (str): Hostname or IP address
            port (int): Port number
            wait (float or bool): if True, will wait for the connection to be established,
                if False, will return immediately,
                if a number, will wait for given number of seconds or until the connection is established,
                whatever happens first
            timeout (float or None): if wait is a number, will keep trying to connect for given number of seconds,
                if None, will keep trying forever (good for server applications)

        Returns:
            severish.base.Task or None: task that will connect to NATS in background,
                or None if connection has been established

        Raises:
            MessengerCannotConnect: if the connection can not be established after `timeout` seconds
                (only if connecting has not been backgrounded)
        """


        if host is None:
            host = self.default_host
        if port is None:
            port = self.default_port
        if self.conn is not None:
            log.warning("Messenger Connection already opened, ignoring")
            return None
        self.conn = ConnectionJetStream(host, port)
        await self.conn.update_statuses()

        self.opener_task = await create_task(self.conn.connect(), f"Messenger NATS connection opener {host}:{port}")
        task = await create_task(self._open(self.conn, timeout), f"Messenger wait for open {host}:{port}")
        if isinstance(wait, bool):
            do_wait = wait
            wait = None
        else:
            do_wait = True
        if do_wait:
            try:
                await task.wait_for(wait, cancel_on_timeout=False)
            except asyncio.TimeoutError as e:
                log.info(f"Not connected in {wait}s. Backgrounding connection to NATS {host}:{port}")
        else:
            log.info(f"Backgrounding connection to NATS {host}:{port}")
        if self.opener_task.done():
            if self.opener_task.cancelled():
                raise MessengerCannotConnect(f"Could not connect to NATS {host}:{port}, gave up after {timeout} seconds")
            else:
                self.opener_task = None
        return self.opener_task


    async def _open(self, con, timeout):
        try:
            await self.opener_task.wait_for(timeout)
        except asyncio.TimeoutError:
            log.warning(f"Time out connecting to NATS {con.host}:{con.port} after {timeout} seconds")
            raise
        except asyncio.CancelledError:
            log.warning(f"Cancelled connecting to NATS {con.host}:{con.port}")
            raise
        log.info(f"Connected to NATS {con.host}:{con.port}")

    async def schedule_open(self, host: str | None = None, port: int | None = None) -> Task:
        # removed from 0.11.0
        raise NotImplementedError('schedule_open has been removed from Messenger from version 0.11.0, use open instead')



    async def close(self):
        if self.conn is not None:
            await self.connection.disconnect()
            self.conn = None

    @property
    def is_open(self) -> bool:
        return self.conn is not None and self.conn.status_ok()

    @contextlib.asynccontextmanager
    async def context(self, host: str | None, port: int | None, timeout: float | None = None):
        """Context manager for connection

        Args:
            host (str): Hostname or IP address
            port (int): Port number
            timeout (float or None): if wait is a number, will keep trying to connect for given number of seconds,

        Returns:
            Messenger: self

        Usage:
            msg = Messenger()
            async with msg.context(host, port) as msg:
                pass # do something
        """
        await self.open(host, port, timeout=timeout)
        try:
            yield self
        finally:
            await self.close()

    def create_meta(self, meta: dict | None = None) -> dict:
        """Creates meta data for a message

        Args:
            meta (dict): meta data to be sent, many metadata will be added automatically

        Returns:
            dict: meta data
        """
        ret = {
            'id': gen_id('msg'),
            "sender": self.name,
            # "receiver": "receiver_name",  # only for direct messages
            'ts': dt_utcnow_array(),
            'trace_level': logging.DEBUG,  # Message trace will be logged if loglevel <= trace_level
            "message_type": "",
            'tags': [],
        }
        if meta is not None:
            ret.update(meta)
        return ret

    def create_msg(self, data: dict | None = None, meta: dict | None = None) -> dict:
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
        msg['meta'] = self.create_meta(meta)
        return msg

    @classmethod
    def unpack_nats_msg(cls, natsmsg: Msg) -> tuple[dict, dict]:
        msg = cls.decode(natsmsg.data)
        data = msg.get('data', {})
        meta = msg.get('meta', {})
        try:
            meta['nats'] = {
                'js': True,
                'seq': natsmsg.metadata.sequence.stream,
                'seq-consumer': natsmsg.metadata.sequence.consumer,
                'stream': natsmsg.metadata.stream,
                'consumer': natsmsg.metadata.consumer,
                'num_delivered': natsmsg.metadata.num_delivered,
                'num_pending': natsmsg.metadata.num_pending,
                'timestamp': dt_ensure_array(natsmsg.metadata.timestamp)
            }
        except nats.errors.NotJSMessageError:
            meta['nats'] = {
                'js': False
            }
        meta['nats'].update({
            'subject': natsmsg.subject,
            'reply': natsmsg.reply,
        })
        return data, meta

    @staticmethod
    def split_msg(msg: dict) -> tuple[dict, dict | None]:
        """Splits message into data and meta

        Warning:
            Method is deprecated, use `unpack_nast_msg which` which inserts additional info (like sec) into the meta

        Args:
            msg (dict): message

        Returns:
            tuple[dict, dict]: data, meta
        """
        log.warning('Messenger.split_msg is deprecated, Use Messenger.unpack_nast_msg instead')
        data = msg.get('data', {})
        meta = msg.get('meta', None)
        return data, meta

    @classmethod
    def msg_to_repr(cls, data: dict, meta: dict) -> str:
        """Converts message to a string representation

        Args:
            data (dict): message data
            meta (dict): message metadata

        Returns:
            str: string representation
        """
        cmeta = meta.copy()
        ts = dt_from_array(cmeta.pop('ts')).strftime("%Y-%m-%dT%H:%M:%S")
        id_ = cmeta.pop('id')
        smeta = ' '.join(f"{k}:{v}" for k, v in cmeta.items())
        sdata = json.dumps(data)
        return f"{ts} {id_} {smeta} data:{sdata[:100]}"

    @classmethod
    def log_msg_trace(cls, data: dict, meta: dict, comment: str) -> None:
        """Logs a message if trace_level is high enough

        Args:
            data (dict): message data
            meta (dict): message metadata
            comment (str): comment to log
        """
        trace_level = meta.get('trace_level', logging.DEBUG)
        if trace_level < log.getEffectiveLevel():
            return
        log.log(trace_level, f"{comment} [{cls.msg_to_repr(data, meta)}]")

    def msg_validate(self, msg: dict):
        """Validates message, raises jsonschema.ValidationError if invalid

        Args:
            msg (dict): message
        """
        if self.validation:
            self.validator.validate(msg)

    @classmethod
    def encode(cls, msg: dict) -> bytes:
        return json.dumps(msg).encode('utf-8')

    @classmethod
    def decode(cls, bdata: bytes) -> dict:
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

    @staticmethod
    def get_progressreader(subject,
                            deliver_policy='last',
                            stop_when_done: bool = True,
                            **kwargs) -> 'MsgProgressReader':
          """Returns a progress reader for a given subject

          Args:
                subject: subject to read from
                deliver_policy: deliver policy, in this context 'last' is most useful for progress tracking
                stop_when_done: whether to stop iteration when all tasks are done

          Returns:
                MsgProgressReader: a progress reader for the given subject
          """

          from serverish.messenger.msg_progress_read import MsgProgressReader
          return MsgProgressReader(subject=subject,
                                    parent=Messenger(),
                                    deliver_policy=deliver_policy,
                                    stop_when_done=stop_when_done,
                                    **kwargs)

    @staticmethod
    def get_journalpublisher(subject) -> 'MsgJournalPublisher':
        """Returns a journal publisher for a given subject

        Args:
            subject (str): subject to publish to

        Returns:
            MsgJournalPublisher: a publisher for the given subject

        """
        from serverish.messenger.msg_journal_pub import MsgJournalPublisher
        return MsgJournalPublisher(subject=subject,
                                   parent=Messenger())

    @staticmethod
    def get_journalreader(subject,
                          deliver_policy='new',
                          **kwargs) -> 'MsgJournalReader':
        """Returns a journal reader for a given subject

        Args:
            subject (str): subject to read from
            deliver_policy (str): deliver policy, one of 'all', 'last', 'new', 'by_start_time', will be passed to consumer config
            kwargs: additional arguments to pass to the reader and underlying NATS consumer config

        Returns:
            MsgJournalReader: a journal reader for the given subject

        """
        from serverish.messenger.msg_journal_read import MsgJournalReader
        return MsgJournalReader(subject=subject,
                                    parent=Messenger(),
                                    deliver_policy=deliver_policy,
                                    **kwargs)


class MsgDriver(Manageable):
    subject: str = param.String(default=None, allow_None=True, doc="User subject to publish to, prefix may be added")
    is_open: bool = param.Boolean(default=False, doc="Has the driver been opened")
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
        self.is_open = True

    async def close(self) -> None:
        self.is_open = False

    @staticmethod
    def ensure_open(func):
        @functools.wraps(func)
        async def wrapper(driver: MsgDriver, *args, **kwargs):
            # If not open, use context manager
            if not driver.is_open:
                async with driver:
                    return await func(driver, *args, **kwargs)
            else:
                return await func(driver, *args, **kwargs)

        return wrapper

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    def __str__(self):
        return f'{self.name} [{self.subject}]'


