import logging
import re
import dns.resolver

from serverish.resource import Resource
from serverish.status import Status

logger = logging.getLogger(__name__.rsplit('.')[-1])


class Connection(Resource):
    """Watches IP connection and reports status"""
    host: str = None
    port: int = None
    regexp_ip: str = r'^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$'
    r_ip: re.Pattern = re.compile(regexp_ip)

    def is_ip(self) -> bool:
        """Returns True if host is just IP address, not a hostname """
        return bool(self.r_ip.match(self.host))

    async def diagnose_dns(self):
        if self.is_ip():
            return {'dns': Status.na}
        try:
            dns.resolver.resolve('dnspython.org', 'MX')
