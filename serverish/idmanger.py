import logging

from serverish.singlethon import Singlethon

logger = logging.getLogger(__name__.rsplit('.')[-1])


def gen_id(name: str):
    """Generates unique ID for the prefix name"""
    ids = IdManager()
    return ids.get_id(name)


class IdManager(Singlethon):
    """Manages unique IDs"""
    ids_counters = {}

    def get_id(self, name: str) -> str:
        """Returns unique ID for the prefix name

        name may contain format specifiers, e.g. 'obj-{:04d}', if present, conter will
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
        """Returns list of unique IDs for the name"""
        return [self.get_id(name) for _ in range(count)]
