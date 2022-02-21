import abc
from codepack.storage import Storable
from typing import Type, Union


class Storage(metaclass=abc.ABCMeta):
    def __init__(self, item_type: Type[Storable] = None):
        if item_type and not issubclass(item_type, Storable):
            raise TypeError(type(item_type))
        self.item_type = item_type

    @abc.abstractmethod
    def init(self, *args, **kwargs):
        """initialize storage"""

    @abc.abstractmethod
    def close(self):
        """close storage"""

    @abc.abstractmethod
    def exist(self, key: Union[str, list]):
        """check if item with 'key' exists"""
