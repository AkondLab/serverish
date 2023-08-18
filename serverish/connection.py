import asyncio
import logging
import re
import socket

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
        self.set_check_methods(ping=self.diagnose_ping, dns=self.diagnose_dns)

    def is_ip(self) -> bool:
        """Returns True if host is just IP address, not a hostname """
        return bool(self.r_ip.match(self.host))

    async def diagnose_dns(self) -> Status:
        """ Diagnoses DNS connection

        Returns:
            Status: Status object, named 'dns'
        """
        if self.is_ip():
            return Status.new_na(msg='IP address, skipping DNS check')
        resolver = aiodns.DNSResolver()
        try:
            await resolver.gethostbyname(self.host, socket.AF_INET)
        except aiodns.error.DNSError as e:
            try:
                return Status.new_fail(msg=f'{self.host}: {e.args[1]}')
            except (IndexError, TypeError, AttributeError):
                return Status.new_fail(msg=f'{self.host}: {e}')

        return Status.new_ok(msg=f'DNS name {self.host} resolved')

    async def diagnose_ping(self) -> Status:
        """Diagnoses ping

        Returns:
            Status: Status object, named 'ping'
        """
        proc = await asyncio.create_subprocess_exec(
            'ping', '-c', '1', self.host,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        _stdout, _stderr = await proc.communicate()
        if proc.returncode == 0:
            return Status.new_ok(msg=f'ping {self.host} successful')
        else:
            return Status.new_fail(msg=f'ping {self.host} failed')

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
