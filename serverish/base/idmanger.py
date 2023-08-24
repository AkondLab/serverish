import logging

from serverish.base.singleton import Singleton

logger = logging.getLogger(__name__.rsplit('.')[-1])


def gen_id(name: str):
    """Generates unique ID for the prefix name"""
    ids = IdManager()
    return ids.get_id(name)


def gen_uid(name: str, length: int = 10):
    """Generates "unique" hex ID of given length with the prefix name

    Uses uuid4 and URL safe base64 encoding to generate unique ID
    """
    return IdManager.get_uid(name, length)


class IdManager(Singleton):
    """Manages unique IDs"""
    ids_counters = {}

    def get_id(self, name: str) -> str:
        """Returns unique ID for the prefix name

        name may contain format specifiers, e.g. 'obj-{:04d}', if present, counter will
        be formatted with the specifier.
        If there is no specifier, if counter is zero, `name` will be returned, otherwise
        `name-<counter>` will be returned.
        """
        if name not in self.ids_counters:
            self.ids_counters[name] = 0
        self.ids_counters[name] += 1

        if '{' not in name:
            if self.ids_counters[name] == 1:
                ret = name
            else:
                ret = f'{name}-{self.ids_counters[name]}'
        else:
            ret = name.format(self.ids_counters[name])
        return ret

    def get_ids(self, name: str, count: int) -> list[str]:
        """Returns generator of unique IDs for the name"""
        for _ in range(count):
            yield self.get_id(name)

    @staticmethod
    def get_uid(name: str, length: int = 10) -> str:
        """Returns "unique" hex ID of given length with the prefix name

        Uses uuid4 and URL safe base64 encoding to generate unique ID
        """
        import uuid
        import base64
        return f'{name}{base64.urlsafe_b64encode(uuid.uuid4().bytes).decode()[:length]}'
