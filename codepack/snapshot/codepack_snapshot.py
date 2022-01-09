from codepack.snapshot import Snapshot


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
        self.__setitem__('argpack', argpack if argpack else dict())

    @classmethod
    def from_dict(cls, d):
        ret = cls()
        for k, v in d.items():
            if k != '_id':
                ret[k] = v
        return ret
