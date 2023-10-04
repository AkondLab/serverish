from __future__ import annotations
import logging
from typing import TYPE_CHECKING

import param


if TYPE_CHECKING:
    from serverish.base.collector import Collector

logger = logging.getLogger(__name__.rsplit('.')[-1])


class Manageable(param.Parameterized):
    """ Manageable object

    Manageable object is an object that can be managed by a 'parent' collector."""

    def __init__(self, name: str = None, parent: Collector = None, **kwargs) -> None:
        self.parent: Collector = parent
        from serverish.base.idmanger import gen_uid, gen_id
        if name is None:
            name = gen_uid(f'{self.__class__.__name__}_')
        else:
            name = gen_id(name)
        super().__init__(name=name, **kwargs)
        if parent is not None:
            self.parent.ensure_parenting(self)

    def remove_parent(self):
        """Removes parent"""
        if self.parent is not None:
            self.parent.remove_child(self)
            self.parent = None

    def set_parent(self, parent: Collector):
        """Sets parent"""
        self.remove_parent()
        self.parent = parent
        self.parent.ensure_parenting(self)