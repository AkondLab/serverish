from param.parameterized import ParameterizedMetaclass

from serverish.base.collector import Collector


class SingletonMeta(ParameterizedMetaclass):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class Singleton(Collector, metaclass=SingletonMeta):
    """Singleton base class. One instance per one concrete class"""
    pass

