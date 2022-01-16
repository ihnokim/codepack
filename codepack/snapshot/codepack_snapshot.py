from codepack.snapshot import Snapshot
from codepack.argpack import ArgPack
from copy import deepcopy


class CodePackSnapshot(Snapshot):
    def __init__(self, codepack=None, argpack=None, timestamp=None):
        if codepack:
            _id = codepack.id
            _serial_number = codepack.serial_number
            _state = codepack.get_state()
            _codes = {k: v.serial_number for k, v in codepack.codes.items()}
            _source = codepack.get_source()
            _structure = codepack.get_structure()
            _subscribe = codepack.subscribe
        else:
            _id = None
            _serial_number = None
            _state = None
            _codes = None
            _source = None
            _structure = None
            _subscribe = None
        super().__init__(id=_id, serial_number=_serial_number, state=_state, timestamp=timestamp)
        self.__setitem__('codes', _codes)
        self.__setitem__('source', _source)
        self.__setitem__('structure', _structure)
        self.__setitem__('subscribe', _subscribe)
        self.set_argpack(argpack=argpack)

    def set_argpack(self, argpack=None):
        if isinstance(argpack, ArgPack):
            tmp = argpack.to_dict()
            tmp.pop('_id', None)
        elif isinstance(argpack, dict):
            tmp = deepcopy(argpack)
        else:
            tmp = dict()
        self.__setitem__('argpack', tmp)

    @classmethod
    def from_dict(cls, d):
        ret = cls()
        for k, v in d.items():
            if k != '_id':
                ret[k] = v
        return ret
