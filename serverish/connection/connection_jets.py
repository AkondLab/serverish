from __future__ import annotations

import json
import logging
from typing import Mapping, Any, Iterable
import re

import nats
import nats.js.errors
from nats.js import JetStreamContext
import param

from serverish.base.exceptions import MessengerKvBucketNotFound, MessengerNotConnected
from serverish.connection.connection_nats import ConnectionNATS
from serverish.connection.nats_errors import nats_errors_translated
from serverish.base.status import StatusReport

logger = logging.getLogger(__name__.rsplit('.')[-1])


class ConnectionJetStream(ConnectionNATS):
    """Watches JetStream connection and reports status"""

    streams = param.Dict(default={},  # {'test': {'subjects': ['test.*']}},  #  {'srvh-s': {'subjects': ['srvh']}},
                         doc='DEPRECATED(Configure streams externally) '
                             '- JetStream streams to be created as mapping of stream name to stream parameters. ',
                         )

    declared_subjects = param.List(default=[], item_type=str,
                                   doc='List of declared subjects which have to be handled by JetStream. '
                                       'It is used for diagnostics, so declaration is not obligatory.')

    js = param.ClassSelector(class_=JetStreamContext, allow_None=True)

    def __init__(self,
                 host: str | Iterable[str], port: int | Iterable[int] = 4222,
                 subject_prefix: str = 'srvh',  # NATS parameters
                 streams: Mapping[str, Mapping[str, Any]] | None = None,  # JetStream parameters
                 **kwargs):
        """Initializes connection watcher

        Args:
            host (str): Hostname(s) or IP address(es)
            port (int): Port number(s)
            subject_prefix (str): Prefix for all subjects
            streams (Mapping[str, Mapping[str, Any]]):
                JetStream streams to be created as mapping of stream name to stream parameters.
                The stream parameters are described in the NATS documentation.
                Usually, the subjects parameter is the one that needs to be set.
        """
        if streams is not None:
            kwargs['streams'] = streams  # to be initialized on param level
        super().__init__(host=host, port=port,
                         subject_prefix=subject_prefix,
                         **kwargs)
        self.add_check_methods(at_beginning=True,
                               # jets_config=self.diagnose_stream_config,
                               jets_subjects=self.diagnose_stream_subjects,
                               jets_strem=self.diagnose_stream_exists,
                               jets_init=self.diagnose_jetstream_init,
                               )

    async def connect(self, **kwargs):
        """Connects to JetStream of the NATS server
        """

        await super().connect(**kwargs)
        nc: nats.NATS | None = self.nc
        if nc is None:
            return
        else:
            self.setup_jetstream()
        # Persist messages on 'foo's subject.
        # await self.js.add_stream(name="sample-stream", subjects=["foo"])
        # await self.js.add_stream(name="sample-stream", subjects=["foo"])
        # await self.js.add_stream(name="dupa", subjects=['srvh.test.js.foo'])
        # await self.js.add_stream(name="dupa2", subjects=['srvh.test.js.foo2'])
        # await self.js.add_stream(name="srvh-s", subjects=['srvh.test.js.foo1'])
        # logger.warning('So far so good')

        for stream, params in self.streams.items():
            try:
                await self.js.add_stream(name=stream, **params)
            except Exception as e:
                logger.error(f"Error creating stream {stream}: {e}")

        await self.update_statuses()

    def setup_jetstream(self):
        if self.nc is not None:
            self.js: nats.js.JetStreamContext = self.nc.jetstream()
        logging.info(f"JetStream connected")

    async def nats_reconnected_cb(self):
        await super().nats_reconnected_cb()
        try:
            self.setup_jetstream()
            await self.update_statuses()
            logging.info(f'JetStream reconnect status: {self.format_status()}')
        except Exception as e:
            logging.error(f'Error setting up JetStream on reconnection cb: {e}')
            # Update status to reflect the error
            await self.update_statuses()

    async def ensure_subject_in_stream(self, stream: str, subject: str,
                                       create_stram_if_needed: bool = False,
                                       move_if_needed: bool = True):
        """Ensures that a subject is in a stream

        Note, that if `stream.config.subjects` contains a wildcarded `test.*` subject,
        then `test.foo` will be accepted as well.

        The method first ensures existence of the stream, if the stream does not exist, it throws an IOError
        or creates it if create_stram_if_needed.
        (We do not create streams on the fly by default, because usually one have to control the stream parameters.)

        If the stream exists, it checks if the subject is in the stream (taking int account wildcards).
        If the subject is not in the stream, checks if subject is in another stream.
        If so, when move_if_needed == False raises IOError, otherwise removes it from there.
        Then adds the subject to the stream of interest.
        """
        js: JetStreamContext = self.js
        try:
            await js.stream_info(stream)
        except nats.errors.Error as e:
            if create_stram_if_needed:
                await js.add_stream(name=stream)
            else:
                raise IOError(f"Stream {stream} does not exist?") from e

        info = await js.stream_info(stream)
        cfg = info.config
        subjects = cfg.subjects
        import fnmatch
        if any(fnmatch.fnmatch(subject, s) for s in subjects):
            return
        # find stream with the subject
        try:
            ast = await js.find_stream_name_by_subject(subject)
        except Exception as e:
            ast = None
        if ast:
            if not move_if_needed:
                raise IOError(f"Subject {subject} is already in stream {ast}")
            ainfo = await js.stream_info(ast)
            acfg = ainfo.config
            acfg.subjects = [s for s in acfg.subjects if not fnmatch.fnmatch(subject, s)]
            await js.update_stream(config=acfg)

        # add subject to stream
        cfg.subjects.append(subject)
        await js.update_stream(config=cfg)

    async def is_subject_persistent(self, subject: str) -> bool:
        """Tells whether messages published to the subject are stored (the subject is covered by a stream)

        Raises:
            MessengerNotConnected: no connection to ask through
            MessengerTransportError: the server could not be asked
        """
        return bool(await self.subject_storages(subject))

    async def subject_storages(self, subject: str) -> list[str]:
        """Returns the names of all streams storing messages of the subject (or of a subject pattern)

        A plain subject is stored in one stream at most; a pattern (``a.b.>``) may
        be spread over several (``a.b.east.>`` here, ``a.b.west.>`` there).

        Raises:
            MessengerNotConnected: no connection to ask through
            MessengerTransportError: the server could not be asked
        """
        js = self._require_js(f"Cannot check where subject '{subject}' is stored")
        with nats_errors_translated(f"Checking where subject '{subject}' is stored failed"):
            # the client library's find_stream_name_by_subject() reports the first match only
            answer = await js._api_request(f"{js._prefix}.STREAM.NAMES",
                                           json.dumps({'subject': subject}).encode(),
                                           timeout=js._timeout)
        return list(answer.get('streams') or [])

    def _require_js(self, context: str) -> JetStreamContext:
        if self.js is None:
            raise MessengerNotConnected(f"{context}, JetStream not connected")
        return self.js

    async def ensure_kv_bucket(self, bucket: str, create_if_needed: bool = False, **config):
        """Binds to a JetStream KV bucket, optionally creating it.

        Like streams (see `ensure_subject_in_stream`), buckets are not created on the fly
        by default, because usually one has to control the bucket parameters.

        Args:
            bucket (str): KV bucket name
            create_if_needed (bool): create the bucket if it does not exist
            config: `nats.js.api.KeyValueConfig` parameters (e.g. history, ttl, max_bytes)
                used only when the bucket is being created

        Returns:
            nats.js.kv.KeyValue: bound KV bucket handle

        Raises:
            MessengerKvBucketNotFound: bucket does not exist and create_if_needed is False
            MessengerNotConnected: no connection to bind through
            MessengerTransportError: any other failure of the underlying NATS client
        """
        js = self._require_js(f"Cannot open KV bucket '{bucket}'")
        with nats_errors_translated(f"Opening KV bucket '{bucket}' failed"):
            try:
                return await js.key_value(bucket)
            except nats.js.errors.BucketNotFoundError as e:
                if not create_if_needed:
                    raise MessengerKvBucketNotFound(
                        f"KV bucket '{bucket}' does not exist, "
                        f"create it externally or open it with create_bucket=True") from e
                logger.info(f"Creating KV bucket {bucket}")
                return await js.create_key_value(bucket=bucket, **config)


    async def diagnose_stream_config(self) -> StatusReport:
        """Diagnoses stream configuration

        Returns:
            Status: Status object
        """
        # No streams here is the new normal
        # if not self.streams:
        #     return Status.fail(msg='Streams config empty')
        try:
            for s in self.streams:
                if re.match(r'^[a-zA-Z0-9_-]+$', s) is None:
                    return StatusReport.error(msg=f'Invalid stream name {s}')
        except TypeError:
            return StatusReport.error(msg='Streams config invalid, can not iterate over it')
        return StatusReport.ok(msg='Streams config valid', deduce_other=False)

    async def diagnose_jetstream_init(self) -> StatusReport:
        """Diagnoses JetStream connection

        Returns:
            Status: Status object
        """
        js: JetStreamContext = self.js
        if js is None:
            return StatusReport.error(msg='Not initialized')
        else:
            return StatusReport.ok(msg='Initialized', deduce_other=False)

    async def diagnose_stream_exists(self) -> StatusReport:
        """Diagnoses any stream existence

        Returns:
            Status: Status object
        """
        js: JetStreamContext = self.js
        if js is None:
            return StatusReport.error(msg='Not initialized')
        try:
            info = await js.streams_info()
            if not info:
                return StatusReport.error(msg='Zero streams in NATS')
        except Exception as e:
            return StatusReport.error(msg=f'Error getting streams info: {e}')
        return StatusReport.ok(msg='Stream(s) exist')

    async def diagnose_stream_subjects(self) -> StatusReport:
        """Diagnoses streams for declared subjects

        Returns:
            Status: Status object
        """
        js: JetStreamContext = self.js
        if js is None:
            return StatusReport.error(msg='Not initialized')
        if not self.declared_subjects:
            return StatusReport.unknown(msg='No declared subjects')
        try:
            for subject in self.declared_subjects:
                try:
                    await js.find_stream_name_by_subject(subject)
                except (LookupError, TypeError) as e:  # That's what nats.py throws...
                    return StatusReport.error(msg=f'Stream for {subject} not found')
        except Exception as e:
            return StatusReport.error(msg=f'Error getting streams info: {e}')
        return StatusReport.ok(msg='All declared subjects have streams')
