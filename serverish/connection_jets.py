import socket
from typing import Sequence

import nats
import param

from serverish.connection import Connection
from serverish.connection_nats import ConnectionNATS
from serverish.status import Status


class ConnectionJetStream(ConnectionNATS):
    js = param.ClassSelector(class_=nats.js.JetStreamContex, allow_None=True)

    def __init__(self, host: str, port: int = 4222, **kwargs):
        """Initializes connection watcher

        Args:
            host (str): Hostname or IP address
            port (int): Port number
        """
        super().__init__(host=host, port=port, **kwargs)
        # self.add_check_methods(at_beginning=True,
        #                        nats=self.diagnose_nats_connected,
        #                        nats_init=self.diagnose_initialized,
        #                        nats_server = self.diagnose_nats_server
        #                        )

    async def connect(self):
        """Connects to JetStream of the NATS server
        """
        nc: nats.NATS | None = self.nc
        self.js = nc.jetstream()
