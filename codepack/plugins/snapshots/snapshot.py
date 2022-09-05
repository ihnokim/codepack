from codepack.storages.storable import Storable
from codepack.plugins.state import State
from typing import Optional, Any, Union, Iterator, KeysView, ValuesView, ItemsView


class Snapshot(Storable):
    def __init__(self, name: Optional[str] = None, serial_number: Optional[str] = None,
                 state: Optional[Union[State, str]] = None,
                 timestamp: Optional[float] = None, **kwargs: Any) -> None:
        super().__init__(name=name, serial_number=serial_number, timestamp=timestamp)
        self.attr = dict()
        self.__setitem__('state', State.get(state))
        for k, v in kwargs.items():
            self.__setitem__(k, v)

    def __setitem__(self, key: str, value: Any) -> None:
        if key == 'state':
            value = State.get(value)
        if key == '_id':
            pass
        elif key == '_serial_number':
            self.set_serial_number(serial_number=value)
        elif key == '_name':
            self.set_name(name=value)
        elif key == '_timestamp':
            self.set_timestamp(timestamp=value)
        else:
            self.attr[key] = value

    def __getitem__(self, item: str) -> Any:
        if item in {'_serial_number', '_id'}:
            return self.get_serial_number()
        elif item == '_name':
            return self.get_name()
        elif item == '_timestamp':
            return self.get_timestamp()
        else:
            return self.attr[item]

    def __getattr__(self, item: str) -> Any:
        return self.__getitem__(item)

    def to_dict(self) -> dict:
        ret = self.get_meta()
        for k, v in self.attr.items():
            ret[k] = v
        if isinstance(ret['state'], State):
            ret['state'] = ret['state'].name
        ret['_id'] = self.get_serial_number()
        return ret

    @classmethod
    def from_dict(cls, d: dict) -> 'Snapshot':
        ret = cls()
        for k, v in d.items():
            if k == '_id':
                continue
            elif k == '_timestamp':
                ret.set_timestamp(timestamp=v)
            elif k == '_serial_number':
                ret.set_serial_number(serial_number=v)
            elif k == '_name':
                ret.set_name(name=v)
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
