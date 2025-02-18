from __future__ import annotations
import logging


from serverish.base.manageable import Manageable

logger = logging.getLogger(__name__.rsplit('.')[-1])

class Collector(Manageable):
    """Has manageable children"""
    # children: list[Manageable] = None

    def __init__(self, name: str = None, parent: Collector = None, **kwargs) -> None:
        super().__init__(name, parent, **kwargs)
        self.children_by_name = {}
        self.children_names = {}

    def ensure_parenting(self, child: Manageable):
        try:
            name = self.children_names[child]
        except KeyError:
            name = child.name
        self.children_by_name[name] = child
        self.children_names[child] = name
        child.parent = self

    def remove_child(self, child: Manageable):
        try:
            name = self.children_names[child]
        except KeyError:
            name = child.name
        del self.children_by_name[name]
        del self.children_names[child]
        child.parent = None

    @property
    def children(self):
        return self.children_by_name.values()
