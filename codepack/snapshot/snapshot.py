from codepack.storage.storable import Storable
from codepack.snapshot.state import State
from datetime import datetime, timezone
from copy import deepcopy


class Snapshot(Storable):
    def __init__(self, id, serial_number, state=None, timestamp=None, **kwargs):
        super().__init__(id=id, serial_number=serial_number)
        self.attr = dict()
        self.__setitem__('id', self.id)
        self.__setitem__('serial_number', self.serial_number)
        self.__setitem__('state', State.get(state))
        self.__setitem__('timestamp', timestamp if timestamp else datetime.now(timezone.utc).timestamp())
        for k, v in kwargs.items():
            self.__setitem__(k, v)

    def __setitem__(self, key, value):
        if key in ['id', 'serial_number']:
            self.__setattr__(key, value)
        if key == 'state':
            value = State.get(value)
        self.attr[key] = value

    def __getitem__(self, item):
        return self.attr[item]

    def __getattr__(self, item):
        return self.__getitem__(item)

    def diff(self, snapshot):
        ret = dict()
        if isinstance(snapshot, self.__class__):
            for k, v in snapshot.items():
                if k not in self.attr:
                    ret[k] = v
                elif v != self.__getitem__(k):
                    ret[k] = v
        elif isinstance(snapshot, dict):
            return self.diff(self.__class__.from_dict(snapshot))
        else:
            raise TypeError(type(snapshot))  # pragma: no cover
        return ret

    def to_dict(self):
        ret = dict()
        for k, v in self.attr.items():
            ret[k] = v
        if isinstance(ret['state'], State):
            ret['state'] = ret['state'].name
        ret['_id'] = ret['serial_number']
        return ret

    @classmethod
    def from_dict(cls, d):
        attr = deepcopy(d)
        attr.pop('_id', None)
        return cls(**attr)

    def __iter__(self):
        return self.attr.__iter__()

    def items(self):
        return self.attr.items()

    def keys(self):
        return self.attr.keys()

    def values(self):
        return self.attr.values()
