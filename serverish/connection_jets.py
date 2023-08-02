from __future__ import annotations

import logging
from typing import Sequence, Mapping, Any
import re

import nats
from nats.js import JetStreamContext
import param

from serverish.connection import Connection
from serverish.connection_nats import ConnectionNATS
from serverish.status import Status

logger = logging.getLogger(__name__.rsplit('.')[-1])


class ConnectionJetStream(ConnectionNATS):
    """Watches JetStream connection and reports status"""
    streams = param.Dict(
        default={'test': {'subjects': ['test.*']}},  #  {'srvh-s': {'subjects': ['srvh']}},
        doc='JetStream streams to be created as mapping of stream name to stream parameters. ',
    )
    js = param.ClassSelector(class_=JetStreamContext, allow_None=True)

    def __init__(self,
                 host: str, port: int = 4222,  # Connection parameters
                 subject_prefix: str = 'srvh',  # NATS parameters
                 streams: Mapping[str, Mapping[str, Any]] | None = None,  # JetStream parameters
                 **kwargs):
        """Initializes connection watcher

        Args:
            host (str): Hostname or IP address
            port (int): Port number
            subject_prefix (str): Prefix for all topics
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
                               # jets=self.diagnose_stream,
                               jets_config=self.diagnose_stream_config,
                               )

    async def connect(self):
        """Connects to JetStream of the NATS server
        """
        await super().connect()
        nc: nats.NATS | None = self.nc
        self.js = nc.jetstream()

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


    async def ensure_subject_in_stream(self, stream: str, subject: str,
                                       move_if_needed: bool = True):
        """Ensures that a subject is in a stream

        Note, that if `stream.config.subjects` conains a wildcarded `test.*` subject,
        then `test.foo` will be accepted as well.

        The method first ensures existence of the stream, if the stream does not exist, it throws an IOError.
        (We do not create streams on the fly, because we want to have control over the stream parameters.)

        If the stream exists, it checks if the subject is in the stream (taking int account wildcards).
        If the subject is not in the stream, checks if subject is in another stream.
        If so, when move_if_needed == False raises IOError, otherwise removes it from there.
        Then adds the subject to the stream of interest.
        """
        js: JetStreamContext = self.js
        try:
            await js.stream_info(stream)
        except nats.errors.Error as e:
            raise IOError(f"Stream {stream} does not exist?") from e

        info = await js.stream_info(stream)
        cfg = info.config
        subjects = cfg.subjects
        import fnmatch
        if any(fnmatch.fnmatch(subject, s) for s in subjects):
            return
        # find stream with the subject
        ast = await js.find_stream_name_by_subject(subject)
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
        if not self.streams:
            return Status.fail(msg='Streams config empty')
        try:
            for s in self.streams:
                if re.match(r'^[a-zA-Z0-9_-]+$', s) is None:
                    return Status.fail(msg=f'Invalid stream name {s}')
        except TypeError:
            return Status.fail(msg='Streams config invalid, can not iterate over it')
        return Status.ok(msg='Streams config valid')
