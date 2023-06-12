import logging
from dataclasses import dataclass

from serverish.manageable import Manageable

logger = logging.getLogger(__name__.rsplit('.')[-1])

@dataclass
class Resource(Manageable):
    """Controls the resource"""
    status: dict[str, str] = None

    def set_status(self, name: str, value: str) -> None:
        """Sets status of the resource"""
        if self.status is None:
            self.status = {}
        self.status[name] = value
