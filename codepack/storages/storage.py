import abc
from codepack.storages.storable import Storable
from typing import Any, Type, Optional, Union


class Storage(metaclass=abc.ABCMeta):
    def __init__(self, item_type: Optional[Type[Storable]] = None, key: str = 'serial_number') -> None:
        if item_type and not issubclass(item_type, Storable):
            raise TypeError(type(item_type))
        self.item_type = item_type
        self.key = key

    @abc.abstractmethod
    def init(self, *args: Any, **kwargs: Any) -> None:
        """initialize storage"""

    @abc.abstractmethod
    def close(self) -> None:
        """close storage"""

    @abc.abstractmethod
    def exist(self, key: Union[str, list], summary: str = '') -> Union[bool, list]:
        """check if item with given key exists"""

    @abc.abstractmethod
    def remove(self, key: Union[str, list]) -> None:
        """remove item with given key"""

    @abc.abstractmethod
    def search(self, key: str, value: Any, projection: Optional[list] = None, to_dict: bool = False) -> list:
        """search by key and value"""

    @abc.abstractmethod
    def list_all(self) -> list:
        """list all keys"""

    @abc.abstractmethod
    def save(self, item: Union[Storable, list], update: bool = False) -> None:
        """save item"""

    @abc.abstractmethod
    def update(self, key: Union[str, list], values: dict) -> None:
        """update item with given key"""

    @abc.abstractmethod
    def load(self, key: Union[str, list], projection: Optional[list] = None, to_dict: bool = False)\
            -> Optional[Union[Storable, dict, list]]:
        """load item with given key"""

    @staticmethod
    def _validate_summary(summary: str) -> tuple:
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
