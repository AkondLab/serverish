from __future__ import annotations
from dataclasses import dataclass
from enum import Enum


class StatusEnum(Enum):
    """Status enum

    Statuses:
        na: not applicable, does not deduce further statuses
        ok: ok, may be used to deduce further `ok` statuses
        fail: fail, does not deduce further statuses
        disabled: disabled, may be used to deduce further `disabled` statuses
    """
    na = "na"
    ok = "ok"
    fail = 'fail'
    disabled = 'disabled'


@dataclass
class Status:
    """Status of the resource

    """
    name: str = 'main'
    status: StatusEnum = StatusEnum.na
    msg: str = None

    # Helpers for easy status construction
    @classmethod
    def na(cls, name: str = 'main', msg: str = None):
        """Returns Status object with status 'na' (not applicable)

        Args:
            name (str, optional): Status name. Defaults to 'main'.
            msg (str, optional): Status message. Defaults to None.

        Returns:
            Status: Status object
        """
        return cls(name, StatusEnum.na, msg)

    @classmethod
    def ok(cls, name: str = 'main', msg: str = None):
        """Returns Status object with status 'ok'

        Args:
            name (str, optional): Status name. Defaults to 'main'.
            msg (str, optional): Status message. Defaults to None.

        Returns:
            Status: Status object
        """
        return cls(name, StatusEnum.ok, msg)

    @classmethod
    def fail(cls, name: str = 'main', msg: str = None):
        """Returns Status object with status 'fail'

        Args:
            name (str, optional): Status name. Defaults to 'main'.
            msg (str, optional): Status message. Defaults to None.

        Returns:
            Status: Status object
        """
        return cls(name, StatusEnum.fail, msg)

    @classmethod
    def disabled(cls, name: str = 'main', msg: str = None):
        """Returns Status object with status 'disabled'

        Args:
            name (str, optional): Status name. Defaults to 'main'.
            msg (str, optional): Status message. Defaults to None.

        Returns:
            Status: Status object
        """
        return cls(name, StatusEnum.disabled, msg)

    @classmethod
    def deduced(cls, name: str, source: Status, msg: str = None):
        """Returns Status object with status deduced  from other

        Args:
            name (str): Status name
            source (Status): Source status
            msg (str, optional): Status message. Defaults to 'Deduced form {src.name}'.

        Returns:
            Status: Status object
        """
        if msg is None:
            msg = f'Deduced form {source.name}'
        return cls(name, source.status, msg)

    def __eq__(self, other):
        if isinstance(other, (StatusEnum, str)):
            return self.status == other
        elif isinstance(other, Status):
            return self.status == other.status and self.name == other.name
        else:
            return False

    def __str__(self):
        return f'{self.name}:{self.status}'

    def __bool__(self):
        return self.status == StatusEnum.ok
