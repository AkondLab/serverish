from typing import Annotated

from dynaconf.utils.boxing import DynaBox

import serverish
from serverish.base import Collector, Status
from serverish.config.defaultconfigloader import DefaultConfigLoader


class Process(Collector):
    def __init__(self):
        super().__init__()
        self.config_loader: DefaultConfigLoader = DefaultConfigLoader()
        self.server_config = DynaBox()

    @property
    def services(self):
        return self.children

    async def startup(self):
        pass

    async def shutdown(self):
        pass

    @property
    def global_config(self) -> DynaBox:
        raise NotImplementedError(f'Global config is not available')

    async def get_io(self, io_name) -> object:
        """As io drivers are usually process-level, we are handling them here

        Base implementation is empty - raises exception that requested io is not available
        """
        raise NotImplementedError(f'IO driver {io_name} is not available')

    def load_server_config(self, filename) -> None:
        self.server_config = self.config_loader.load_config_from_file(filename)

    def set_server_config(self, config: dict) -> None:
        self.server_config = DynaBox(config)

    async def advertise_service(self,
                                svc_domain: str, svc_name: str, svc_instance: str,
                                access: Annotated[dict, 'server info for accesing the service'],
                                status: Status,
                                ) -> None:
        """Advertise service to the world"""
        pass

    async def create_task(self, coro, name: str) -> serverish.Task:
        pass

    async def ensure_global_config(self):
        pass
