import logging

from serverish.manageable import Manageable

class Singlethon(Manageable):
    """Singleton"""

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Singlethon, cls).__new__(cls)
        return cls.instance