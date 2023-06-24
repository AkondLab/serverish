import socket
from typing import Sequence

import param

from nats.aio.client import Client as NATS

from serverish.connection import Connection
from serverish.status import Status


class ConnectionNATS(Connection):
    nc = param.ClassSelector(class_=NATS, allow_None=True)

    def __init__(self, host: str, port: int = 4222, **kwargs):
        """Initializes connection watcher

        Args:
            host (str): Hostname or IP address
            port (int): Port number
        """
        super().__init__(host=host, port=port, **kwargs)
        self.add_check_methods(at_beginning=True,
                               nats=self.diagnose_nats_connected,
                               nats_init=self.diagnose_initialized,
                               nats_server = self.diagnose_nats_server
                               )

    async def connect(self):
        """Connects to NATS server

        This method is not obligatory, one can just set self.nc to a connected NATS object,
        also provide url and port in that case to have properly working status checks.
        """
        if self.nc is None:
            self.nc = NATS()
        await self.nc.connect(servers=[f'nats://{self.host}:{self.port}'])

    async def disconnect(self):
        """Disconnects from NATS server"""
        if self.nc is not None:
            await self.nc.close()
            self.nc = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()


    async def diagnose_initialized(self) -> Status:
        """Diagnoses initialization

        Returns:
            Status: Status object
        """
        if self.nc is None:
            return Status.fail(msg='Not initialized')
        return Status.ok(msg='Initialized')


    async def diagnose_nats_connected(self) -> Status:
        """Diagnoses NATS connection
        Returns:
            Status: Status object, named 'nats'
        """
        if self.nc is None:
            return Status.fail(msg='Not initialized')
        if not self.nc.is_connected:
            return Status.fail(msg='Not connected')
        return Status.ok(msg='Connected')

    async def diagnose_nats_server(self) -> Status:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((self.host, self.port))
            s.shutdown(socket.SHUT_RDWR)
            return Status.ok(msg=f'Server is running at {self.host}:{self.port}')
        except ConnectionRefusedError:
            return Status.fail(msg=f'Connection to {self.host}:{self.port} refused')
        finally:
            s.close()
