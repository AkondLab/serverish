from __future__ import annotations
import asyncio
import logging
import re
import socket
from typing import Iterable

import param
from serverish.base.hasstatuses import HasStatuses
from serverish.base.status import Status

logger = logging.getLogger(__name__.rsplit('.')[-1])


class Connection(HasStatuses):
    """Watches IP connection and reports status"""
    host = param.List(default=['localhost'], item_type=str)
    port = param.List(default=[80], item_type=int)

    regexp_ip: str = r'^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$'
    r_ip: re.Pattern = re.compile(regexp_ip)

    def __init__(self, host: str|Iterable[str], port: int|Iterable[int], **kwargs):
        """Initializes connection watcher

        Args:
            host (str): Hostname or IP address, may be multiple
            port (int): Port number, may be multiple

        If both host and port are iterable, they must have same length.
        If host is iterable and port is not, port is repeated for each host.
        If port is iterable and host is not, host is repeated for each port.
        """
        if isinstance(host, str):
            host = [host]
        else:
            host = list(host)
        if isinstance(port, int):
            port = [port]
        else:
            port = list(port)
        if len(host) == 1 and len(port) > 1:
            host = host * len(port)
        if len(port) == 1 and len(host) > 1:
            port = port * len(host)
        if len(host) != len(port):
            raise ValueError(f'hosts and ports must have same length, got {len(host)} and {len(port)}')

        super().__init__(host=host, port=port, **kwargs)
        self.set_check_methods(ping=self.diagnose_ping, dns=self.diagnose_dns)



    @classmethod
    def is_ip(cls, host) -> bool:
        """Returns True if host is just IP address, not a hostname """
        return bool(cls.r_ip.match(host))


    @staticmethod
    async def _check_host(resolver, h):
        import aiodns
        try:
            await resolver.gethostbyname(h, socket.AF_INET)
            return None
        except aiodns.error.DNSError as e:
            try:
                return f'{h}: {e.args[1]}'
            except (IndexError, TypeError, AttributeError):
                return f'{h}: {e}'


    async def diagnose_dns(self) -> Status:
        """ Diagnoses DNS connection

        Returns:
            Status: Status object, named 'dns'
        """
        try:
            import aiodns
        except ImportError:
            return Status.new_na(msg='aiodns not installed, skipping DNS check')


        if len(self.host) == 0:
            return Status.new_na(msg='No hosts to check DNS for')

        resolver = aiodns.DNSResolver()
        tasks = [self._check_host(resolver, h) for h in self.host if not self.is_ip(h)]
        results = await asyncio.gather(*tasks)

        checked = len(results)
        failed = [res for res in results if res is not None]

        if checked == 0:
            return Status.new_na(msg='No DNS names to check')
        elif len(failed) == checked:
            return Status.new_fail(msg=f'DNS check failed for all {checked} names: {", ".join(failed)}')
        elif len(failed) > 0:
            return Status.new_ok(msg=f'DNS check ok for {checked - len(failed)} of {checked} names, '
                                     f'but failed for: {", ".join(failed)}')
        else:
            return Status.new_ok(msg=f'DNS check ok for all {checked} names')

    async def diagnose_ping(self) -> Status:
        """Diagnoses ping

        Returns:
            Status: Status object, named 'ping'
        """

        async def _ping_host(host):
            proc = await asyncio.create_subprocess_exec(
                'ping', '-c', '1', host,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE)
            _stdout, _stderr = await proc.communicate()
            return (host, proc.returncode)

        if len(self.host) == 0:
            return Status.new_na(msg='No hosts to ping')

        tasks = [_ping_host(host) for host in self.host]
        results = await asyncio.gather(*tasks)

        successful_pings = [host for host, returncode in results if returncode == 0]
        failed_pings = [host for host, returncode in results if returncode != 0]

        if len(successful_pings) == len(self.host):
            return Status.new_ok(msg=f'Ping successful for all hosts: {", ".join(successful_pings)}')
        elif len(successful_pings) > 0:
            return Status.new_ok(msg=f'Ping ok for {len(successful_pings)} of {len(self.host)} hosts, '
                                     f'but failed for: {", ".join(failed_pings)}')
        else:
            return Status.new_fail(msg=f'Ping failed for all {len(self.host)} hosts: {", ".join(failed_pings)}')

    def create_urls(self, protocol: str='http', path: str='') -> list[str]:
        """Creates list of URLs from host and port

        Args:
            protocol (str): Protocol to use, default 'http'
            path (str): Path to append to URL, default ''

        Returns:
            list[str]: List of URLs
        """
        return [f'{protocol}://{h}:{p}{path}' for h, p in zip(self.host, self.port)]


    # async def diagnose_dns2(self) -> dict[str, Status]:
    #
    #         """Diagnoses DNS connection
    #
    #     Returns:
    #         dict[str, Status]: dictionary with 'dns' key and Status object
    #     """
    #     if self.is_ip():
    #         return {'dns': Status.na('IP address, skipping DNS check')}
    #     try:
    #         dns.resolver.resolve(self.host, 'A')
    #     except dns.resolver.NXDOMAIN:
    #         return {'dns': Status.fail(f'DNS name {self.host} not found')}
    #     except dns.resolver.NoNameservers:
    #         return {'dns': Status.fail('No network or DNS server found')}
    #     except dns.resolver.NoAnswer:
    #         return {'dns': Status.fail('No answer from DNS server')}
    #     except dns.resolver.Timeout:
    #         return {'dns': Status.fail('DNS query timeout')}
    #     except dns.resolver.DNSException:
    #         return {'dns': Status.fail('Unknown DNS error')}
    #     return {'dns': Status.ok('DNS name {self.host} resolved')}
