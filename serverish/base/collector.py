import logging

import param

from serverish.base.manageable import Manageable

logger = logging.getLogger(__name__.rsplit('.')[-1])

class Collector(Manageable):
    """Has manageable children"""
    # children: list[Manageable] = None
    children_by_name: dict[str, Manageable] = param.Dict(default=dict(), allow_None=False, doc='Children by name')
    children_names: dict[Manageable, str] = param.Dict(default=dict(), allow_None=False, doc='Names by child')

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
