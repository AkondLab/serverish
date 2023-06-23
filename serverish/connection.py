import asyncio
import logging
import re
import socket
from typing import Sequence

import aiodns
import param

from serverish.hasstatuses import HasStatuses
from serverish.status import Status

logger = logging.getLogger(__name__.rsplit('.')[-1])


class Connection(HasStatuses):
    """Watches IP connection and reports status"""
    host = param.String()
    port = param.Integer(default=80, bounds=(1, 65535))

    regexp_ip: str = r'^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$'
    r_ip: re.Pattern = re.compile(regexp_ip)

    def __init__(self, host: str, port: int, **kwargs):
        """Initializes connection watcher

        Args:
            host (str): Hostname or IP address
            port (int): Port number
        """
        super().__init__(host=host, port=port, **kwargs)
        self.check_methods = [self.diagnose_ping, self.diagnose_dns]

    def is_ip(self) -> bool:
        """Returns True if host is just IP address, not a hostname """
        return bool(self.r_ip.match(self.host))

    async def diagnose_dns(self, deduce_from: Sequence[Status] | None = None) -> Status:
        """ Diagnoses DNS connection

        Args:
            deduce_from (Sequence[Status], optional): Statuses to deduce from. Defaults to None.

        Returns:
            Status: Status object, named 'dns'
        """
        if deduce_from is not None:   # try to deduce from main. If main is OK or disabled, take it.
            for s in deduce_from:
                if s.name in ['main', 'ping'] and s in [Status.ok, Status.na]:
                    return Status.deduced('dns', s)
        if self.is_ip():
            return Status.na('dns', msg='IP address, skipping DNS check')
        resolver = aiodns.DNSResolver()
        try:
            await resolver.gethostbyname(self.host, socket.AF_INET)
        except aiodns.error.DNSError as e:
            try:
                return Status.fail('dns', msg=f'{self.host}: {e.args[1]}')
            except (IndexError, TypeError, AttributeError):
                return Status.fail('dns', msg=f'{self.host}: {e}')

        return Status.ok('dns', msg=f'DNS name {self.host} resolved')

    async def diagnose_ping(self, deduce_from: Sequence[Status] | None = None) -> Status:
        """Diagnoses ping

        Args:
            deduce_from (Sequence[Status], optional): Statuses to deduce from. Defaults to None.

        Returns:
            Status: Status object, named 'ping'
        """
        if deduce_from is not None:
            for s in deduce_from:
                if s.name in ['main'] and s in [Status.ok, Status.na]:
                    return Status.deduced('ping', s)
        proc = await asyncio.create_subprocess_exec(
            'ping', '-c', '1', self.host,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        _stdout, _stderr = await proc.communicate()
        if proc.returncode == 0:
            return Status.ok('ping', msg=f'ping {self.host} successful')
        else:
            return Status.fail('ping', msg=f'ping {self.host} failed')

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
