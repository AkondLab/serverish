import os.path
from functools import lru_cache
import logging

from dynaconf.utils.boxing import DynaBox

import serverish
from serverish.base import Status
from serverish.messenger import Messenger
from serverish.services.process import Process


class ServerishProcess(Process):

    def __init__(self):
        super().__init__()
        self.messenger = None
        self._global_config: DynaBox | None = None
        self.log = logging.getLogger(self.name)

    async def get_io(self, io_name):
        """As io drivers are usually process-level, we are handling them here
        """
        match io_name:
            case 'messenger':
                return await self.ensure_messenger()
            case _:  # pragma: no cover
                raise ValueError(f'Unknown io name: {io_name}')

    @property
    def global_config(self):
        if self._global_config is None:
            raise RuntimeError('Global config is not loaded')
        return self._global_config


    async def startup(self):
        await self.ensure_messenger()


    async def shutdown(self):
        if self.messenger is not None:
            await self.messenger.close()
            self.messenger = None

    @lru_cache(maxsize=15)
    def get_publisher(self, subject):
        return self.messenger.get_publisher(subject)

    async def advertise_service(self, svc_domain: str, svc_name: str, svc_instance: str, access: dict, status: Status) -> None:
        """Implementation publishes service existence to NATS discovery channel"""
        subject = self.server_config.get('nats.subjects.svc_discovery', None)
        if subject is None:
            return
        try:
            publisher = self.get_publisher(subject)
        except Exception as e:
            self.log.error(f'Cannot get publisher for {subject}: {e}')
            return
        data = {
            'domain': svc_domain,
            'service': svc_name,
            'instance': svc_instance,
            'status': status.to_dict(),
            'access': access
        }
        try:
            await publisher.publish(data)
        except Exception as e:
            self.log.error(f'Cannot publish to {subject}: {e}')
            return

    async def create_task(self, coro: callable, name: str):
        t = await serverish.create_task(coro, name)
        return t

    async def ensure_messenger(self):
        if self.messenger is None:
            self.messenger = Messenger()  # this is singleton anyway
            if not self.messenger.is_open:
                host = self.server_config.get('nats.host', 'localhost')
                port = self.server_config.get('nats.port', 4222)
                await self.messenger.open(host=host, port=port)
                self.log.info('Messenger opened')

        return self.messenger

    async def ensure_global_config(self):
        if self._global_config is None:
            subject = self.server_config.get('nats.subjects.global_config', 'site.config')
            self._global_config = await self.config_loader.load_config_from_messenger(
                subject,
                fallbackfile=os.path.expanduser('~/.config/serverish/global_config_fallback.yaml')
            )
            self.log.info('Global config loaded')



