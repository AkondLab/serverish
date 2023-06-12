import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from serverish.collector import Collector

logger = logging.getLogger(__name__.rsplit('.')[-1])


class Manageable:

    def __init__(self, id: str = None, parent: 'Collector' = None) -> None:
        self.parent: Collector = parent
        super().__init__()
        if parent is not None:
            self.parent.ensure_parenting(self)