from codepack.abc import Storable
from datetime import datetime
from codepack.utils.state_code import StateCode
from copy import deepcopy


class Snapshot(Storable):
    def __init__(self, id, serial_number, state=None, timestamp=None, **kwargs):
        super().__init__(id=id, serial_number=serial_number)
        self.attr = dict()
        self.__setitem__('id', self.id)
        self.__setitem__('serial_number', self.serial_number)
        self.__setitem__('state', StateCode.get(state))
        self.__setitem__('timestamp', timestamp if timestamp else datetime.now().timestamp())
        for k, v in kwargs.items():
            self.__setitem__(k, v)

    def __setitem__(self, key, value):
        if key in ['id', 'serial_number']:
            self.__setattr__(key, value)
        self.attr[key] = value

    def __getitem__(self, item):
        return self.attr[item]

    def __getattr__(self, item):
        return self.__getitem__(item)

    def diff(self, snapshot):
        ret = dict()
        if isinstance(snapshot, self.__class__):
            return self.diff(snapshot=snapshot.to_dict())
        elif isinstance(snapshot, dict):
            for k, v in snapshot.items():
                if k not in self.attr:
                    ret[k] = v
                elif v != self.__getitem__(k):
                    ret[k] = v
        else:
            raise TypeError(type(snapshot))  # pragma: no cover
        return ret

    def to_dict(self, *args, **kwargs):
        return deepcopy(self.attr)

    @classmethod
    def from_dict(cls, d, *args, **kwargs):
        attr = deepcopy(d)
        return cls(*args, **attr, **kwargs)

    def __iter__(self):
        return self.attr.__iter__()

    def items(self):
        return self.attr.items()

    def keys(self):
        return self.atttr.keys()

    def values(self):
        return self.attr.values()
