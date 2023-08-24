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
        if name is None:
            from serverish.base.idmanger import gen_uid
            name = gen_uid('manageable')
        super().__init__(name=name, **kwargs)
        if parent is not None:
            self.parent.ensure_parenting(self)
