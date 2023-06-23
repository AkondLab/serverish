from __future__ import annotations
import asyncio
import logging
from typing import Callable, Sequence, Awaitable

import param

from serverish.collector import Collector
from serverish.manageable import Manageable
from serverish.status import Status

logger = logging.getLogger(__name__.rsplit('.')[-1])


class HasStatuses(Manageable):
    """Controls the resource

    Parameters
    ----------
    status : dict[str, Status], optional
        Statuses of the resource, by default None

    """
    @staticmethod
    async def diagnose_dummy_ok(deduce_from: Sequence[Status] | None = None) -> Status:  # noqa: F841
        """Dummy diagnosis, sets 'main' status to OK

        Args:
            deduce_from (Sequence[Status], optional): Statuses to deduce from. Defaults to None.
            """
        return Status.ok('main', msg='Dummy OK')

    status = param.Dict(default=None, allow_None=True, doc='A dictionary to hold status information')

    def __init__(self, name: str = None, parent: Collector = None, **kwargs) -> None:
        self.check_methods: Sequence[Callable[[Sequence[Status] | None], Awaitable[Status]]] = (self.diagnose_dummy_ok,)

        super().__init__(name, parent, **kwargs)

    def set_check_methods(self, *methods: Callable[[Sequence[Status] | None], Awaitable[Status]]) -> None:
        """Sets check methods

        Methods are called in order, values of later methods may be deduced from earlier ones.
        E.g. if 'ping' is OK, we deduce that 'dns' is OK too.
        One can force all diagnotics to be run by setting no_deduce=True in diagnose() call.


        Args:
            *methods (Callable[[Sequence[Status] | None], Awaitable[Status]]): Methods to set
        """
        self.check_methods = methods

    def add_check_methods(self, at_beginning: bool,
                          *methods: Callable[[Sequence[Status] | None], Awaitable[Status]]) -> None:
        """Adds check methods

        Methods are called in order, values of later methods may be deduced from earlier ones.
        E.g. if 'ping' is OK, we deduce that 'dns' is OK too.
        One can force all diagnotics to be run by setting no_deduce=True in diagnose() call.

        Args:
            at_beginning (bool): If True, adds methods at the beginning of the list.
            *methods (Callable[[Sequence[Status] | None], Awaitable[Status]]): Methods to add
        """
        if at_beginning:
            self.check_methods = methods + self.check_methods
        else:
            self.check_methods = self.check_methods + methods

    def set_status(self, value: Status) -> None:
        """Sets status of the resource

        A resource can have multiple statuses, e.g. 'main', 'dns', 'http', etc.
        Args:
            value (Status): Status object
        """
        if self.status is None:
            self.status = {}
        self.status[value.name] = value

    async def diagnose(self, no_deduce: bool = False) -> Sequence[Status]:
        """Diagnoses the object

        Diagnose by calling check methods and setting statuses.
        Methods are called in order, values of later methods may be deduced from earlier ones.
        E.g. if 'ping' is OK, we deduce that 'dns' is OK too.
        One can force all diagnotics to be run by setting no_deduce=True.

        Args:
            no_deduce (bool, optional): If True, don't deduce statuses from other statuses. Defaults to False.
        """
        if no_deduce:
            checking_tasks = [m(None) for m in self.check_methods]
            res = await asyncio.gather(*checking_tasks)
        else:
            res = []
            for m in self.check_methods:
                r = await m(res)
                res.append(r)
        return res

    async def update_statuses(self) -> None:
        """Updates statuses of the resource

        Calls diagnose() and sets statuses
        """
        statuses = await self.diagnose()
        for s in statuses:
            self.set_status(s)
