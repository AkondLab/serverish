import os
from abc import abstractmethod, ABC

class BaseServer(ABC):

    config_file_locations = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'settings', 'default_server.yaml'),
        '/usr/local/etc/serverish/server.yaml',
        os.path.expanduser('~/.config/serverish/server.yaml'),
        os.path.join('.', 'server.yaml')
    ]

    @abstractmethod
    async def run(self):
        pass

