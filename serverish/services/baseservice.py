from abc import ABC, abstractmethod
import asyncio
from asyncio import Event
from typing import Optional, Dict, Any
import serverish
from serverish.base.collector import Collector
from serverish.base import Status, StatusEnum
from serverish.base import Manageable
from serverish.base import HasStatuses

class ServiceCommunicator(ABC):
    pass


class BaseService(HasStatuses, ABC):
    """Base class for services"""

    def __init__(
            self,
            name: str,
            instance_id: str,
            communicator: ServiceCommunicator,
            parent: Optional[Collector] = None,
            **kwargs
    ):
        self.instance_id = instance_id
        self.communicator = communicator
        self._stop_event = Event()
        self._cleaned_up_event = Event()
        self.running = False

        super().__init__(name=name, parent=parent, **kwargs)

        # Setup basic status checks
        self.set_check_methods(
            main=self._check_main_status,
            comm=self._check_comm_status,
        )

    async def run(self):
        """Start the service"""
        try:
            self.running = True
            self._stop_event.clear()
            self._cleaned_up_event.clear()

            # Start status monitoring
            await serverish.create_task(self._status_monitor(), name=f'{self.name} status_monitor')

            # Start service-specific tasks
            await self._start_service()

            # Wait for stop signal
            await self._cleaned_up_event.wait()

        except Exception as e:
            self.set_status('main', Status.new_fail(msg=str(e)))
            raise

    async def stop(self):
        """Stop the service"""
        self._stop_event.set()
        self.running = False
        await self._stop_service()
        self._cleaned_up_event.set()

    async def _status_monitor(self):
        """Periodic status updates"""
        while self.running:
            await self.update_statuses()
            await self.communicator.publish_status(
                self.name,
                self.telescope_id,
                self.status
            )
            await asyncio.sleep(5)

    async def _check_main_status(self) -> Status:
        """Check main service status"""
        if not self.running:
            return Status.new_fail(msg="Service not running")
        return Status.new_ok()

    async def _check_comm_status(self) -> Status:
        """Check communication status"""
        if not self.communicator.is_connected():
            return Status.new_fail(msg="Communication lost")
        return Status.new_ok()

    async def _check_telescope_status(self) -> Status:
        """Check telescope connection status"""
        # To be implemented by concrete services
        return Status.new_na(msg="Not implemented")

    @abstractmethod
    async def _start_service(self):
        """Service-specific startup logic"""
        pass

    @abstractmethod
    async def _stop_service(self):
        """Service-specific cleanup logic"""
        pass
