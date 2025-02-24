import asyncio
import logging

from serverish.base import Status
from serverish.services import Service
from serverish.services.serverishprocess import ServerishProcess
from serverish.services.serverishservicecontroller import ServerishServiceController
from serverish.services.servers.base import BaseServer
from serverish.services.utils import load_service_class, get_ext_ip_and_host

_log = logging.getLogger('server')

class SimpleServer(BaseServer):

    def __init__(self):
        self.service_controllers: list[ServerishServiceController] = []
        # get info about server machine and network
        ip, host = get_ext_ip_and_host()

        self.machine_info = {'ip': ip}
        if host:
            self.machine_info['host'] = host


    async def run(self, server_config: dict | None = None):
        """No fancy logic, just runs all known services in single process"""

        _log.info('Starting server')
        proc = ServerishProcess()

        if server_config is None:
            proc.load_server_config(self.config_file_locations)
        else:
            proc.set_server_config(server_config)

        await proc.startup()
        _log.info('Shared subsystems are started')

        await self.create_services(proc)
        _log.info(f'{len(self.service_controllers)} services are created')

        await self.start_services(proc)
        _log.info('Services are started')

        await self.run_services_tasks(proc)
        _log.info('Services are running')

        await self.wait_for_services(proc)
        _log.info('Services finished')

        await proc.shutdown()
        _log.info('Shared subsystems are stopped')

    async def create_services(self, proc: ServerishProcess):
        svc_conf = proc.server_config.get('services', {})

        service_defs = {}

        # collect services definitions
        # iterate domains
        for svc_domain, domain_conf in svc_conf.items():
            # iterate services
            for svc_name, svc_conf in domain_conf.items():
                # iterate instances
                for svc_instance in svc_conf.get('instances', [""]):
                    service_defs[(svc_domain, svc_name, svc_instance)] = svc_conf
        _log.info(f'Found {len(service_defs)} service instance definitions')

        # create services
        for (svc_domain, svc_name, svc_instance), svc_conf in service_defs.items():
            if not svc_conf.get('enabled', False):
                _log.info(f'Service {Service.format_dni(svc_domain, svc_name, svc_instance)} is disabled')
                Status.new_na('disabled')
                await proc.advertise_service(svc_domain, svc_name, svc_instance,
                                             access=self.machine_info, status=Status.new_disabled('disabled'))
                continue
            svc_cls = None
            try:
                class_module = svc_conf['class']['module']
                class_name = svc_conf['class']['name']
            except LookupError as e:
                _log.error(f'Cannot find class definition of service '
                           f'{Service.format_dni(svc_domain, svc_name, svc_instance)} '
                           f'in service config: {svc_conf}, {e}')
                await proc.advertise_service(svc_domain, svc_name, svc_instance,
                                             access=self.machine_info, status=Status.new_fail('config error'))
                continue
            try:
                svc_cls = load_service_class(class_module, class_name)
            except ImportError:
                _log.error(f'Cannot import service module {class_module} for service '
                           f'{Service.format_dni(svc_domain, svc_name, svc_instance)}')
            except AttributeError:
                _log.error(f'Cannot find class {class_name} in module {class_module} for service '
                           f'{Service.format_dni(svc_domain, svc_name, svc_instance)}')
            except TypeError:
                _log.error(f'Class {class_name} in module {class_module} is not a subclass of Service '
                           f'{Service.format_dni(svc_domain, svc_name, svc_instance)}')
            except Exception as e:
                _log.error(f'Error loading service {Service.format_dni(svc_domain, svc_name, svc_instance)}: {e}')
            finally:
                if svc_cls is None:
                    await proc.advertise_service(svc_domain, svc_name, svc_instance,
                                             access=self.machine_info, status=Status.new_fail('config error'))
                    continue
            controller = ServerishServiceController(proc)
            try:
                await controller.setup_service(svc_cls, svc_domain, svc_name, svc_instance, **svc_conf.get('kwargs', {}))
            except Exception as e:
                _log.error(f'Cannot setup service {Service.format_dni(svc_domain, svc_name, svc_instance)}: {e}')
                await proc.advertise_service(svc_domain, svc_name, svc_instance,
                                             access=self.machine_info, status=Status.new_fail('config error'))
                continue
            self.service_controllers.append(controller)

    async def stop(self):
        """Stop all services"""
        await self.stop_services()

    async def start_services(self, proc: ServerishProcess):
        for controller in self.service_controllers:
            try:
                await controller.startup_service()
            except Exception as e:
                _log.error(f'Cannot start service {controller.service}: {e}')
                await proc.advertise_service(controller.service.svc_domain, controller.service.svc_name, controller.service.svc_instance,
                                             access=self.machine_info, status=Status.new_fail('start error'))

    async def run_services_tasks(self, proc):
        for controller in self.service_controllers:
            try:
                await controller.run_service_task()
            except Exception as e:
                _log.error(f'Error in service {controller.service}: {e}')
                await proc.advertise_service(controller.service.svc_domain, controller.service.svc_name, controller.service.svc_instance,
                                             access=self.machine_info, status=Status.new_fail('run error'))
            await proc.advertise_service(controller.service.svc_domain, controller.service.svc_name, controller.service.svc_instance,
                                         access=self.machine_info, status=Status.new_ok('running'))

    async def stop_services(self):
        async def stop_service(controller: ServerishServiceController):
            try:
                await controller.stop()
            except Exception as e:
                _log.error(f'Error stopping service {controller.service}: {e}')
        # stop all together
        await asyncio.gather(*[stop_service(controller) for controller in self.service_controllers])

    async def wait_for_services(self, proc):
        async def wait_for_service(controller: ServerishServiceController):
            if controller.do_task is None:
                return
            try:
                await controller.do_task
            except Exception as e:
                _log.error(f'Error in service {controller.service}.do: {e}')
                await proc.advertise_service(controller.service.svc_domain, controller.service.svc_name, controller.service.svc_instance,
                                             access=self.machine_info, status=Status.new_fail('run error'))
            await proc.advertise_service(controller.service.svc_domain, controller.service.svc_name, controller.service.svc_instance,
                                         access=self.machine_info, status=Status.new_na('finished'))
            await controller.cleanup_service()

        await asyncio.gather(*[wait_for_service(controller) for controller in self.service_controllers])








