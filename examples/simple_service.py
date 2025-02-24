import asyncio
import logging
from serverish.services import Service
from serverish.services.servicecontroller import ServiceController



class SimpleService(Service):

    class Settings:
        needs_global_config = True
        # config_file_locations = [
        #     'settings/simple_service.yaml',
        #     '/usr/local/etc/{domain}.{name}/config.yaml',
        #     '~/.config/{domain}.{name}/config.yaml'
        # ]
        # needs_io = ['messenger']

    async def startup(self, controller: ServiceController):
        controller.log.info('Simple Service is starting')
        # Check controller capabilities
        gc = controller.global_config
        controller.log.info(f'Global config has {len(gc)} keys')
        cfg = controller.config
        controller.log.info(f'Simple Service config has {len(cfg)} keys')

    async def do(self, controller: ServiceController):
        controller.log.info('Simple Service will work for 10s')
        await asyncio.sleep(10)
        controller.log.info('Simple Service finished working')

    async def cleanup(self, controller: ServiceController):
        controller.log.info('Simple Service cleaning up')


if __name__ == '__main__':
    # run minimalistic server with single service
    # argumens:
    # nats_host - NATS server host
    # nats_port - NATS server port
    # global_config_subject - NATS subject for global config
    # svc_discovery_subject - NATS subject for service discovery
    SimpleService.run_dev()

        





