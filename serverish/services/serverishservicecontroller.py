from logging import Logger, getLogger
from typing import Any, Union

from dynaconf.utils.boxing import DynaBox

import serverish
from serverish.services import Service
from serverish.services.process import Process
from serverish.services.servicecontroller import ServiceController


class ServerishServiceController(ServiceController):

    def __init__(self, process: Process):
        self.do_task: serverish.Task | None = None
        self._log = getLogger('svcctl')
        self.process: Process = process
        self._service: Service | None = None
        self.service_setup = False
        super().__init__()
        self._io = {}
        self.service_config: dict | None = None

    @property
    def service(self):
        return self._service

    async def setup_service(self, svc_cls: type[Service],
                            svc_domain: str, svc_name: str, svc_instance: str|None, **kwargs):
        self._log = getLogger(Service.format_dni(svc_domain, svc_name, svc_instance))
        # Instantiate service
        self._service = svc_cls(svc_domain, svc_name, svc_instance, parent=self.process, **kwargs)
        self.log.info('Service instance created')
        # Load service config
        filename = None
        for filename_template in self.svc_class_setting('config_file_locations'):
            try:
                filename = filename_template.format(domain=svc_domain, name=svc_name, instance=svc_instance)
            except LookupError:
                self.log.error(f'Wrong "settings file filename template" for {self._service}: {filename_template}')
        if filename is None:
            self.log.error(f'No settings file found for {self._service}')
            raise RuntimeError(f'No settings file found for {self._service}')
        self.log.info(f'Loading settings from {filename}')
        try:
            self.service_config = self.process.config_loader.load_config_from_file(filename)
        except Exception as e:
            self.log.error(f'Failed to load settings from {filename}: {e}')
            self.service_config = DynaBox({})

        # Check if global config is needed
        if self.svc_class_setting('needs_global_config'):
            await self.process.ensure_global_config()

        # Prepare io (by asking self.process)
        for io_name in self.svc_class_setting('needs_io'):
            self._io[io_name] = await self.process.get_io(io_name)
        self.log.info('IO drivers are ready')
        self.service_setup = True

    async def startup_service(self):
        if not self.service_setup:
            return
        await self.service.startup(self)
        # call base implementation
        await super().startup_service()
        self.log.info('Service started')

    async def run_service_task(self):
        if not self.service_started:
            return
        self.do_task = await self.process.create_task(self.service.do(self), name=self.service.name + '.do')
        # call base implementation
        await super().run_service_task()
        self.log.info('Service working')

    async def cleanup_service(self):
        if not self.service_setup:
            return
        await self.service.cleanup(self)
        # call base implementation
        await super().cleanup_service()
        self.log.info('Service stopped')

    async def stop(self, wait: bool = False):
        self.service.stop_requested.set()
        self.log.info('Stop requested')
        if wait:
            await self.do_task



    def svc_class_setting(self, key) -> Any:
        try:
            return self.service.Settings.__dict__[key]
        except LookupError:
            return self.service.DefaultSettings.__dict__[key]

    @property
    def io(self) -> dict:
        """Returns dictionary with various communication objects

        e.g. controller.io['messenger'].get_rpcresponder(controller.config['rpc.listen.subject'])
        """
        return self._io

    @property
    def config(self) -> dict:
        """Service config, usually read from /usr/local/etc/servicename/config.yaml"""
        return self.service_config

    @property
    def global_config(self) -> dict:
        """Global settings, usually read from NATS"""
        needed = self.svc_class_setting('needs_global_config')
        if needed:
            return self.process.global_config
        else:
            self.log.error(f'Global config requested, but {type(self._service).__name__}.Settings.needs_global_config not set')
            raise RuntimeError(f'The need for global config is not set for {self._service}')


    @property
    def log(self) -> Logger:
        """Properly named logger for service"""
        return self._log



    def include_router(self, router: Union['APIRouter', 'MessengerRouter']):
        """Adds FastApi Router or Messenger Router

        e.g.
        from fastapi import APIRouter, HTTPException, status, Body
        router = APIRouter()

        @router.get("/{id}", response_description="Get a single Object", response_model=Object)
        async def get_object(id: PydanticObjectId):
        controller.include_router(router)
        """
        pass


