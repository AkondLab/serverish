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

    Attributes:
        status (StatusEnum): Status
        msg (str): Status message
        deduce_other (bool): Whether to deduce other statuses without checking from this one
    """
    status: StatusEnum = StatusEnum.na
    msg: str = None
    deduce_other: bool = True

    # Helpers for easy status construction
    @classmethod
    def new_na(cls, msg: str = None, deduce_other: bool = False) -> Status:
        """Returns Status object with status 'na' (not applicable)

        Args:
            msg (str, optional): Status message. Defaults to None.
            deduce_other (bool, optional): Whether to deduce other statuses without checking from this one.
                                           Defaults to False.

        Returns:
            Status: Status object
        """
        return cls(StatusEnum.na, msg, deduce_other=deduce_other)

    @classmethod
    def new_ok(cls, msg: str = None, deduce_other: bool = True) -> Status:
        """Returns Status object with status 'ok'

        Args:
            msg (str, optional): Status message. Defaults to None.
            deduce_other (bool, optional): Whether to deduce other statuses without checking from this one.
                                           Defaults to True.

        Returns:
            Status: Status object
        """
        return cls(StatusEnum.ok, msg, deduce_other=deduce_other)

    @classmethod
    def new_fail(cls, msg: str = None, deduce_other: bool = False) -> Status:
        """Returns Status object with status 'fail'

        Args:
            msg (str, optional): Status message. Defaults to None.
            deduce_other (bool, optional): Whether to deduce other statuses without checking from this one.
                                           Defaults to False.

        Returns:
            Status: Status object
        """
        return cls(StatusEnum.fail, msg, deduce_other=deduce_other)

    @classmethod
    def new_disabled(cls, msg: str = None, deduce_other: bool = False) -> Status:
        """Returns Status object with status 'disabled'

        Args:
            msg (str, optional): Status message. Defaults to None.
            deduce_other (bool, optional): Whether to deduce other statuses without checking from this one.
                                           Defaults to False.

        Returns:
            Status: Status object
        """
        return cls(StatusEnum.disabled, msg, deduce_other=deduce_other)

    @classmethod
    def deduced(cls, source: Status, msg: str = None):
        """Returns Status object with status deduced  from other

        Args:
            source (Status): Source status
            msg (str, optional): Status message. Defaults to 'Deduced form {src.name}'.
            deduce_other (bool, optional): Whether to deduce other statuses without checking from this one.
                                           Defaults to False.

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

    def __repr__(self):
        return f"[{self.status.name}] {self.msg})"


    def __bool__(self):
        return self.status == StatusEnum.ok
