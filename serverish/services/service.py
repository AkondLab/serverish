import asyncio
import os
from abc import ABC, abstractmethod
from asyncio import Event

from serverish.base import HasStatuses, Collector
from serverish.services.servicecontroller import ServiceController


class Service(ABC, HasStatuses):
    """Base class for serverish services

    Override any subset of methods start, do, stop.
    Default implementation of `do` method, just blocks
    until service is shout down
    """
    class DefaultSettings:
        needs_global_config = False
        config_file_locations = [
            os.path.join(os.path.dirname(os.path.abspath(__file__)), 'settings', 'default_service.yaml'),
            '/usr/local/etc/{domain}.{name}/config.yaml',
            os.path.expanduser('~/.config/{domain}.{name}/config.yaml')
            ]

        needs_io = ['messenger']

    # Override to provide own settings values
    Settings = DefaultSettings


    def __init__(self, svc_domain: str, svc_name: str, svc_instance: str | None = None,
                 parent: Collector | None = None, **kwargs
                 ) -> None:
        fullname = self.format_dni(svc_domain, svc_name, svc_instance)
        super().__init__(fullname, parent)
        self.svc_domain = svc_domain
        self.svc_name = svc_name
        self.svc_instance = svc_instance
        self.stop_requested: Event = Event()

    @abstractmethod
    async def startup(self, controller: ServiceController):
        """Service startup logic"""
        pass

    @abstractmethod
    async def do(self, controller: ServiceController):
        """Service main logic

        Exiting this method finishes service. Default implementation waits for stop request.
        """
        await self.stop_requested.wait()

    @abstractmethod
    async def cleanup(self, controller: ServiceController):
        """Service cleanup logic"""
        pass

    @staticmethod
    def format_dni(domain, svc_name, instance):
        """Returns 'domain.name.instance' or 'domain.name' if no instance """
        if instance:
            return f'{domain}.{svc_name}.{instance}'
        else:
            return f'{domain}.{svc_name}'

    @classmethod
    def run_dev(cls):
        """Run this single service in simple server in development mode

        Parses following command line arguments:
            --nats_host
            --nats_port
            --global_config_subject
            --svc_discovery_subject
        """
        import logging
        import fire
        import sys
        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('dev')
        log.info('Development mode, available parameters: --nats_host, --nats_port, --global_config_subject, --svc_discovery_subject')
        log.info(f'Starting {cls.__name__} in development mode')
        module = cls.__module__
        if module == '__main__':
            module = os.path.abspath(sys.argv[0])
        def create_config(
                nats_host='localhost',
                nats_port=4222,
                global_config_subject='site.config',
                svc_discovery_subject='site.services'
        ):
            svc_name = cls.__name__.lower()
            cfg = {
                'nats': {
                    'host': nats_host,
                    'port': nats_port,
                    'subjects': {
                        'global_config': global_config_subject,
                        'svc_discovery': svc_discovery_subject,
                    }
                },
                'services': {
                    'site': {
                        svc_name: {
                            'instances': ['main'],
                            'class': {
                                'name': cls.__name__,
                                'module': module
                            },
                            'enabled': True,
                            'kwargs': {}
                        }
                    }
                }
            }
            return cfg

        config = fire.Fire(create_config, serialize=lambda x: "")
        log.info(f'Actual Config: {config}')
        from serverish.services import server
        try:
            asyncio.run(server.run(server_config=config))
        except KeyboardInterrupt:
            pass
