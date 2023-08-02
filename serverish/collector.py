import logging
from dataclasses import dataclass

from serverish.manageable import Manageable

logger = logging.getLogger(__name__.rsplit('.')[-1])

class Collector(Manageable):
    """Has manageable children"""
    children: list[Manageable] = None

    def ensure_parenting(self, child: Manageable):
        if self.children is None:
            self.children = []
        if child not in self.children:
            self.children.append(child)
        child.parent = self
