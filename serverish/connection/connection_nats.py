from __future__ import annotations

import asyncio
import logging
import socket
from typing import Iterable, Tuple

import param

from nats.aio.client import Client as NATS

from serverish.connection import Connection
from serverish.base.status import Status

_logger = logging.getLogger(__name__.rsplit('.')[-1])


class ConnectionNATS(Connection):
    """Watches NATS connection and reports status"""
    subject_prefix = param.String(default='srvh')
    nc = param.ClassSelector(class_=NATS, allow_None=True)

    def __init__(self, host: str|Iterable[str], port: int|Iterable[int] = 4222,
                 subject_prefix: str = 'srvh',
                 **kwargs):
        """Initializes connection watcher

        Args:
            host (str): Hostname or IP address
            port (int): Port number
        """
        super().__init__(host=host, port=port,
                         subject_prefix=subject_prefix,
                         **kwargs)
        self.add_check_methods(at_beginning=True,
                               nats_op = self.diagnose_nats_server_op,
                               nats_connected=self.diagnose_nats_connected,
                               nats_init=self.diagnose_initialized,
                               nats_server = self.diagnose_nats_server_port,
                               )
        self.reconnect_cbs = []
        # self.status['nats'] = Status.new_na(msg='Not initialized')

    async def nats_error_cb(self, e: Exception):
        """Error callback for NATS connection"""
        await self.update_statuses()
        _logger.debug(f'NATS error: {e}, Status: {self.format_status()}')

    async def nats_disconnected_cb(self):
        """Disconnected callback for NATS connection"""
        await self.update_statuses()
        _logger.info(f'NATS disconnected')

    async def nats_reconnected_cb(self):
        """Reconnected callback for NATS connection"""
        await self.update_statuses()
        _logger.info(f'NATS reconnected: Status: {self.format_status()}')
        for cb in self.reconnect_cbs:
            await cb()

    async def nats_closed_cb(self):
        """Closed callback for NATS connection"""
        await self.update_statuses()
        _logger.info(f'NATS closed')

    def add_reconnect_cb(self, cb):
        self.reconnect_cbs.append(cb)

    def remove_reconnect_cb(self, cb):
        try:
            self.reconnect_cbs.remove(cb)
        except ValueError:
            pass

    async def connect(self, **kwargs):
        """Connects to NATS server

        This method is not obligatory, one can just set self.nc to a connected NATS object,
        also provide url and port in that case to have properly working status checks.
        """
        if self.nc is None:
            self.nc = NATS()

        # prepare some our-default options for connect
        async def cb_nats_error_relay(e):
            await self.nats_error_cb(e)

        async def cb_nats_disconnected_relay():
            await self.nats_disconnected_cb()

        async def cb_nats_reconnected_relay():
            await self.nats_reconnected_cb()

        async def cb_nats_closed_relay():
            await self.nats_closed_cb()


        kwargs.setdefault('error_cb', cb_nats_error_relay)
        kwargs.setdefault('disconnected_cb', cb_nats_disconnected_relay)
        kwargs.setdefault('reconnected_cb', cb_nats_reconnected_relay)
        kwargs.setdefault('closed_cb', cb_nats_closed_relay)
        kwargs.setdefault('name', 'serverish')

        kwargs.setdefault('max_reconnect_attempts', -1)

        try:
            await self.nc.connect(servers=self.create_urls(protocol='nats'), **kwargs)
        except Exception as e:
            _logger.error(f'NATS connection failed: {e}')
            self.nc = None
            raise e
        finally:
            await self.update_statuses()

    async def disconnect(self):
        """Disconnects from NATS server"""
        if self.nc is not None:
            if self.nc.is_connected:
                await self.nc.close()
            self.nc = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()


    def diagnose_initialized(self) -> Status:
        """Diagnoses initialization

        Returns:
            Status: Status object
        """
        if self.nc is None:
            return Status.new_fail(msg='Not initialized')
        return Status.new_ok(msg='Initialized', deduce_other=False)

    def diagnose_nats_connected(self) -> Status:
        """Diagnoses NATS connection
        Returns:
            Status: Status object, named 'nats'
        """
        if self.nc is None:
            return Status.new_fail(msg='Not initialized')
        if not self.nc.is_connected:
            return Status.new_fail(msg='Not connected')
        return Status.new_ok(msg='Connected')

    async def diagnose_nats_server_port(self) -> Status:
        async def _check_port(host, port) -> Tuple[str, str | None]:
            loop = asyncio.get_event_loop()
            try:
                # loop.sock_connect to do it asynchronously
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.setblocking(False)
                await loop.sock_connect(s, (host, port))
                s.close()
                return f"{host}:{port}", None
            except ConnectionRefusedError:
                return f"{host}:{port}", f'Connection to {host}:{port} refused'
            except asyncio.CancelledError:
                raise
            except Exception as e:
                return f"{host}:{port}", f'Error: {e}'

        if len(self.host) == 0:
            return Status.new_fail(msg='No host specified')

        tasks = [_check_port(host, port) for host, port in zip(self.host, self.port)]
        results = await asyncio.gather(*tasks)

        successful_connections = [host_port for host_port, status in results if status is None]
        failed_connections = [host_port for host_port, status in results if status is not None]

        if len(successful_connections) == len(tasks):
            return Status.new_ok(msg=f'Connected on all {len(tasks)} addresses: {", ".join(successful_connections)}')
        elif len(successful_connections) > 0:
            return Status.new_ok(msg=f'Connected to {len(successful_connections)} of {len(tasks)}. '
                                       f'Failed to connect to: {", ".join(failed_connections)}')
        else:
            return Status.new_fail(msg=f'Failed to connect to any of {len(tasks)} addresses: '
                                       f'{", ".join(failed_connections)}')

    async def diagnose_nats_server_op(self) -> Status:
        if self.nc is None:
            return Status.new_fail(msg='Not initialized')
        if not self.nc.is_connected:
            return Status.new_fail(msg='Not connected')
        try:
            await self.nc.publish('test.ping', b'')
            return Status.new_ok(msg='Operational')
        except Exception as e:
            return Status.new_fail(msg=f'Not operational: {e}')





