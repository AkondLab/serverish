from __future__ import annotations
import logging
from typing import TYPE_CHECKING, Any
from collections.abc import Mapping

if TYPE_CHECKING:
    from serverish.base.collector import Collector

logger = logging.getLogger(__name__.rsplit('.')[-1])


class Manageable:
    """ Manageable object

    Manageable object is an object that can be managed by a 'parent' collector."""

    def __init__(self, name: str | None = None, parent: Collector | None = None, **kwargs) -> None:
        self.parent: Collector | None = parent
        from serverish.base.idmanger import gen_uid, gen_id
        if name is None:
            name = gen_uid(f'{self.__class__.__name__}_')
        else:
            name = gen_id(name)
        self.name: str = name
        super().__init__()
        if parent is not None:
            self.parent.ensure_parenting(self)

    def remove_parent(self) -> None:
        """Removes parent"""
        if self.parent is not None:
            self.parent.remove_child(self)
            self.parent = None

    def set_parent(self, parent: Collector) -> None:
        """Sets parent"""
        self.remove_parent()
        self.parent = parent
        self.parent.ensure_parenting(self)

    def __str__(self) -> str:
        """String representation showing name and class"""
        return f"{self.name} ({self.__class__.__name__})"

    def __repr__(self) -> str:
        """Detailed representation including attributes"""
        args = self._get_repr_args()
        return f"{self.__class__.__name__}({args})"

    def _get_repr_args(self) -> str:
        """Get the arguments for repr, excluding private and special attributes"""
        attrs = []
        for name, value in self.__dict__.items():
            if name.startswith('_'):
                continue
            if isinstance(value, (str, bytes)):
                value = repr(value)
            attrs.append(f"{name}={value}")
        return ", ".join(attrs)

    def get_param_values(self) -> dict[str, Any]:
        """Returns a dictionary of parameter names and their values

        Excludes private attributes (starting with '_')
        """
        return {name: value for name, value in self.__dict__.items()
                if not name.startswith('_')}

    def param_updates(self, params: Mapping[str, Any]) -> None:
        """Update multiple parameters at once using a dictionary

        Args:
            params: Dictionary of parameter name/value pairs
        """
        for name, value in params.items():
            setattr(self, name, value)