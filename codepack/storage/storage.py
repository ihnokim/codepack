import abc
from codepack.storage import Storable


class Storage(metaclass=abc.ABCMeta):
    def __init__(self, obj=None):
        if isinstance(obj, Storable):
            raise TypeError(type(obj))
        self.obj = obj

    @abc.abstractmethod
    def init(self, *args, **kwargs):
        """initialize storage"""
