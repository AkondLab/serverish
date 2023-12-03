from __future__ import annotations
import asyncio
import logging
from typing import Callable, Awaitable, Union

import param

from serverish.base.collector import Collector
from serverish.base.manageable import Manageable
from serverish.base.status import Status, StatusEnum

logger = logging.getLogger(__name__.rsplit('.')[-1])

CheckMethodType = Callable[[], Union[Awaitable[Status], Status]]

class HasStatuses(Manageable):
    """Controls the resource

    Parameters
    ----------
    status : dict[str, Status], optional
        Statuses of the resource, by default None

    """
    @staticmethod
    def diagnose_dummy_ok() -> Status:
        """Dummy diagnosis, returns StatusEnum.ok"""
        return Status.new_ok(msg='Dummy OK')

    status = param.Dict(default={}, doc='A dictionary to hold status information')

    def __init__(self, name: str = None, parent: Collector = None, **kwargs) -> None:
        self.check_methods: dict[str, CheckMethodType] = {'main': self.diagnose_dummy_ok}

        super().__init__(name, parent, **kwargs)

    def set_check_methods(self, **methods: CheckMethodType) -> None:
        """Sets check methods

        Methods are called in order, values of later methods may be deduced from earlier ones:
        if some method returns StatusEnum.ok, or StatusEnum.na, we deduce that the result of all later methods
        is the same.
        E.g. if 'ping' is OK, we deduce that 'dns' is OK too.
        One can force all diagnotics to be run by setting no_deduce=True in diagnose() call.


        Args:
            **methods (dict[str, CheckMethodType]): Dict of asynchronous methods
        """
        self.check_methods = methods

    def add_check_methods(self, at_beginning: bool,
                          **methods: CheckMethodType) -> None:
        """Adds check methods

        Methods are called in order, values of later methods may be deduced from earlier ones:
        if some method returns StatusEnum.ok, or StatusEnum.na, we deduce that the result of all later methods
        is the same.
        E.g. if 'ping' is OK, we deduce that 'dns' is OK too.
        One can force all diagnotics to be run by setting no_deduce=True in diagnose() call.

        Args:
            at_beginning (bool): If True, adds methods at the beginning of the list.
            **methods (dict[str, Callable]): Dict of asynchronous methods to add
        """
        if at_beginning:
            self.check_methods = {**methods, **self.check_methods}
        else:
            self.check_methods = {**self.check_methods, **methods}

    def set_status(self, key: str, value: Status | None) -> None:
        """Sets status of the resource

        A resource can have multiple statuses, e.g. 'main', 'dns', 'http', etc.
        Args:
            key (str): Status name
            value (Status): Status object
        """
        if self.status is None:
            self.status = {}
        if value is None:
            self.status.pop(key, None)
        else:
            self.status[key] = value

    async def diagnose(self, no_deduce: bool = False) -> dict[str, Status]:
        """Diagnoses the object

        Diagnose by calling check methods and setting statuses.
        Methods are called in order, values of later methods may be deduced from earlier ones:
        if some method returns StatusEnum.ok, or StatusEnum.na, we deduce that the result of all later methods
        is the same.
        E.g. if 'ping' is OK, we deduce that 'dns' is OK too.

        One can force all diagnotics to be run by setting no_deduce=True.

        Args:
            no_deduce (bool, optional): If True, don't deduce statuses from other statuses. Defaults to False.
        """

        async def calc_state(m):
            if asyncio.iscoroutinefunction(m):
                return await m()
            else:
                return m()

        if no_deduce:
            checking_tasks = [calc_state(m) for m in self.check_methods.values()]
            res = await asyncio.gather(*checking_tasks)
        else:
            res = []
            deduced_status: Status | None = None
            deduced_name: str | None = None
            for n, m in self.check_methods.items():
                if deduced_status is not None:
                    r = Status.deduced(deduced_status, f'Deduced from {deduced_name}')
                else:
                    r = await calc_state(m)
                    if r.deduce_other:
                        deduced_status = r
                        deduced_name = n
                res.append(r)
        return {n: r for n, r in zip(self.check_methods.keys(), res)}

    def diagnose_sync(self, no_deduce: bool = False) -> dict[str, Status]:
        """Diagnoses the object skipping async methods

        Diagnose by calling check methods and setting statuses.
        Methods are called in order, values of later methods may be deduced from earlier ones:
        if some method returns StatusEnum.ok, or StatusEnum.na, we deduce that the result of all later methods
        is the same.
        E.g. if 'ping' is OK, we deduce that 'dns' is OK too.

        One can force all diagnotics to be run by setting no_deduce=True.

        Args:
            no_deduce (bool, optional): If True, don't deduce statuses from other statuses. Defaults to False.
        """

        def calc_state(m):
            if asyncio.iscoroutinefunction(m):
                return Status.new_na(msg='Skipped async test')
            else:
                return m()

        if no_deduce:
            res = [calc_state(m) for m in self.check_methods.values()]
        else:
            res = []
            deduced_status: Status | None = None
            deduced_name: str | None = None
            for n, m in self.check_methods.items():
                if deduced_status is not None:
                    r = Status.deduced(deduced_status, f'Deduced from {deduced_name}')
                else:
                    r = calc_state(m)
                    if r.deduce_other:
                        deduced_status = r
                        deduced_name = n
                res.append(r)
        return {n: r for n, r in zip(self.check_methods.keys(), res)}

    async def update_statuses(self, no_deduce = False) -> None:
        """Updates statuses of the resource

        Args:
            no_deduce (bool, optional): If True, don't deduce statuses from other statuses. Defaults to False.

        Calls diagnose() and sets statuses
        """
        statuses = await self.diagnose(no_deduce=no_deduce)
        for n, s in statuses.items():
            self.set_status(n, s)

    def update_sync_statuses(self, no_deduce = False) -> None:
        """Updates statuses of the resource skipping async methods

        Args:
            no_deduce (bool, optional): If True, don't deduce statuses from other statuses. Defaults to False.

        Calls diagnose() and sets statuses
        """
        statuses = self.diagnose_sync(no_deduce=no_deduce)
        for n, s in statuses.items():
            self.set_status(n, s)

    def point_of_failure(self) -> (str | None, Status | None):
        """Returns the most low-level failed status - most probably the point of failure

        Returns:
            (str, Status): Name and status of the most low-level failed status
        """

        for k, s in reversed(self.status.items()):
            if s == StatusEnum.fail:
                return k, s
        return None, None

    def format_status(self) -> str:
        n, s = self.point_of_failure()
        if s is None:
            return 'OK'
        else:
            return f'Failed {n} ({s.msg})'

    def status_ok(self) -> bool:
        """Returns True if no failed statuses"""
        return self.point_of_failure()[1] is None
