from __future__ import annotations

import logging
from typing import Mapping, Any, Iterable
import re

import nats
from nats.js import JetStreamContext
import param

from serverish.connection.connection_nats import ConnectionNATS
from serverish.base.status import Status

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
        self.setup_jetstream()
        await self.update_statuses()
        logging.info(f'Jeststream reconnect status: {self.format_status()}')

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


    async def diagnose_stream_config(self) -> Status:
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
                    return Status.new_fail(msg=f'Invalid stream name {s}')
        except TypeError:
            return Status.new_fail(msg='Streams config invalid, can not iterate over it')
        return Status.new_ok(msg='Streams config valid', deduce_other=False)

    async def diagnose_jetstream_init(self) -> Status:
        """Diagnoses JetStream connection

        Returns:
            Status: Status object
        """
        js: JetStreamContext = self.js
        if js is None:
            return Status.new_fail(msg='Not initialized')
        else:
            return Status.new_ok(msg='Initialized', deduce_other=False)

    async def diagnose_stream_exists(self) -> Status:
        """Diagnoses any stream existence

        Returns:
            Status: Status object
        """
        js: JetStreamContext = self.js
        if js is None:
            return Status.new_fail(msg='Not initialized')
        try:
            info = await js.streams_info()
            if not info:
                return Status.new_fail(msg='Zero streams in NATS')
        except Exception as e:
            return Status.new_fail(msg=f'Error getting streams info: {e}')
        return Status.new_ok(msg='Stream(s) exist')

    async def diagnose_stream_subjects(self) -> Status:
        """Diagnoses streams for declared subjects

        Returns:
            Status: Status object
        """
        js: JetStreamContext = self.js
        if js is None:
            return Status.new_fail(msg='Not initialized')
        if not self.declared_subjects:
            return Status.new_na(msg='No declared subjects')
        try:
            for subject in self.declared_subjects:
                try:
                    await js.find_stream_name_by_subject(subject)
                except (LookupError, TypeError) as e:  # That's what nats.py throws...
                    return Status.new_fail(msg=f'Stream for {subject} not found')
        except Exception as e:
            return Status.new_fail(msg=f'Error getting streams info: {e}')
        return Status.new_ok(msg='All declared subjects have streams')
