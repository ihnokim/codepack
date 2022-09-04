from codepack.storages.storable import Storable
from codepack.plugins.state import State
from typing import Optional, Any, Union, Iterator, KeysView, ValuesView, ItemsView


class Snapshot(Storable):
    def __init__(self, id: Optional[str] = None, serial_number: Optional[str] = None,
                 state: Optional[Union[State, str]] = None,
                 timestamp: Optional[float] = None, **kwargs: Any) -> None:
        super().__init__(id=id, serial_number=serial_number, timestamp=timestamp)
        self.attr = dict()
        self.__setitem__('state', State.get(state))
        for k, v in kwargs.items():
            self.__setitem__(k, v)

    def __setitem__(self, key: str, value: Any) -> None:
        if key == 'state':
            value = State.get(value)
        if key in {'serial_number', '_id'}:
            self.serial_number = value
        elif key == 'id':
            self.set_id(id=value)
        elif key == '_timestamp':
            self.set_timestamp(timestamp=value)
        else:
            self.attr[key] = value

    def __getitem__(self, item: str) -> Any:
        if item in {'serial_number', '_id'}:
            return self.serial_number
        elif item == 'id':
            return self.get_id()
        elif item == '_timestamp':
            return self.get_timestamp()
        else:
            return self.attr[item]

    def __getattr__(self, item: str) -> Any:
        return self.__getitem__(item)

    def to_dict(self) -> dict:
        ret = dict()
        for k, v in self.attr.items():
            ret[k] = v
        if isinstance(ret['state'], State):
            ret['state'] = ret['state'].name
        ret['_id'] = self.serial_number
        ret['id'] = self.get_id()
        ret['serial_number'] = self.serial_number
        ret['_timestamp'] = self.get_timestamp()
        return ret

    @classmethod
    def from_dict(cls, d: dict) -> 'Snapshot':
        ret = cls()
        for k, v in d.items():
            if k == 'id':
                ret.set_id(id=v)
            if k == '_timestamp':
                ret.set_timestamp(timestamp=v)
            elif k == 'serial_number':
                ret.serial_number = v
            elif k == '_id':
                continue
            else:
                ret[k] = v
        return ret

    def __iter__(self) -> Iterator:
        return self.attr.__iter__()

    def items(self) -> ItemsView:
        return self.attr.items()

    def keys(self) -> KeysView:
        return self.attr.keys()

    def values(self) -> ValuesView:
        return self.attr.values()
