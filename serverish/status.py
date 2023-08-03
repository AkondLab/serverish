from __future__ import annotations
from dataclasses import dataclass
from enum import Enum

try:
    from enum import StrEnum
except ImportError:
    class StrEnum(str, Enum):
        pass


class StatusEnum(StrEnum):
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
    status: StatusEnum = StatusEnum.na
    msg: str = None

    # Helpers for easy status construction
    @classmethod
    def na(cls, msg: str = None):
        """Returns Status object with status 'na' (not applicable)

        Args:
            msg (str, optional): Status message. Defaults to None.

        Returns:
            Status: Status object
        """
        return cls(StatusEnum.na, msg)

    @classmethod
    def ok(cls, msg: str = None):
        """Returns Status object with status 'ok'

        Args:
            msg (str, optional): Status message. Defaults to None.

        Returns:
            Status: Status object
        """
        return cls(StatusEnum.ok, msg)

    @classmethod
    def fail(cls,  msg: str = None):
        """Returns Status object with status 'fail'

        Args:
            msg (str, optional): Status message. Defaults to None.

        Returns:
            Status: Status object
        """
        return cls(StatusEnum.fail, msg)

    @classmethod
    def disabled(cls, msg: str = None):
        """Returns Status object with status 'disabled'

        Args:
            msg (str, optional): Status message. Defaults to None.

        Returns:
            Status: Status object
        """
        return cls(StatusEnum.disabled, msg)

    @classmethod
    def deduced(cls, source: Status, msg: str = None):
        """Returns Status object with status deduced  from other

        Args:
            source (Status): Source status
            msg (str, optional): Status message. Defaults to 'Deduced form {src.name}'.

        Returns:
            Status: Status object
        """
        if msg is None:
            msg = f'Deduced'
        return cls(source.status, msg)

    def __eq__(self, other):
        if isinstance(other, (StatusEnum, str)):
            return self.status == other
        elif isinstance(other, Status):
            return self.status == other.status
        else:
            return False

    def __str__(self):
        return f'{self.status}'

    def __bool__(self):
        return self.status == StatusEnum.ok
