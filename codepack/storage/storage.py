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
    def exist(self, key: Union[str, list], summary: str = ''):
        """check if item with given key exists"""

    @abc.abstractmethod
    def remove(self, key: Union[str, list]):
        """remove item with given key"""

    @abc.abstractmethod
    def search(self, key: str, value: object, projection: list = None):
        """search by key and value"""

    @staticmethod
    def _validate_summary(summary):
        op = summary.lower()
        if op == '':
            init = list()
        elif op == 'and':
            init = True
        elif op == 'or':
            init = False
        else:
            raise ValueError('%s is unknown' % summary)
        return op, init
