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
        default={'srvh-s': {'subjects': ['srvh']}},
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
        await self.js.add_stream(name="sample-stream", subjects=["foo"])
        await self.js.add_stream(name="sample-stream", subjects=["foo"])
        await self.js.add_stream(name="dupa", subjects=['srvh.test.js.foo'])
        await self.js.add_stream(name="dupa2", subjects=['srvh.test.js.foo2'])
        await self.js.add_stream(name="srvh-s", subjects=['srvh.test.js.foo1'])
        logger.warning('So far so good')

        for stream, params in self.streams.items():
            try:
                await self.js.add_stream(name=stream, **params)
            except Exception as e:
                logger.error(f"Error creating stream {stream}: {e}")




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
