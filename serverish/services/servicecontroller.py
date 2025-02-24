from logging import Logger
from abc import ABC, abstractmethod
from typing import Union


class ServiceController(ABC):
    """Utility methods provided to service"""

    def __init__(self):
        super().__init__()
        self.service_started = False  # after startup
        self.service_working = False  # inside service.do

    @property
    @abstractmethod
    def io(self) -> dict:
        """Returns dictionary with various communication objects

        e.g. controller.io['messenger'].get_rpcresponder(controller.config['rpc.listen.subject'])
        """
        pass

    @property
    @abstractmethod
    def config(self) -> dict:
        """Service config, usually read from /usr/local/etc/servicename/config.yaml"""
        pass

    @property
    @abstractmethod
    def global_config(self) -> dict:
        """Global config, usually read from NATS"""
        pass

    @property
    @abstractmethod
    def log(self) -> Logger:
        """Properly named logger for service"""
        pass

    @abstractmethod
    def include_router(self, router: Union['APIRouter','MessengerRouter']):
        """Adds FastApi Router or Messenger Router

        e.g.
        from fastapi import APIRouter, HTTPException, status, Body
        router = APIRouter()

        @router.get("/{id}", response_description="Get a single Object", response_model=Object)
        async def get_object(id: PydanticObjectId):
        controller.include_router(router)
        """
        pass

    async def startup_service(self):
        """Starts service

        This method should be called by overriden start_service implementation
        """
        self.service_started = True

    async def run_service_task(self):
        """Runs service task

        This method should be called by overriden run_service_task implementation
        """
        self.service_working = True

    async def cleanup_service(self):
        """Cleans up service

        This method should be called by overriden cleanup_service implementation
        """
        self.service_working = False
        self.service_started = False



