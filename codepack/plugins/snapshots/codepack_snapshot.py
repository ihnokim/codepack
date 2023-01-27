from codepack.plugins.snapshots.snapshot import Snapshot
from codepack.argpack import ArgPack
from copy import deepcopy
from typing import TypeVar, Optional, Union


CodePack = TypeVar('CodePack', bound='codepack.codepack.CodePack')  # noqa: F821


class CodePackSnapshot(Snapshot):
    def __init__(self, codepack: Optional[CodePack] = None, argpack: Optional[Union[ArgPack, dict]] = None,
                 timestamp: Optional[float] = None) -> None:
        if codepack:
            _name = codepack.get_name()
            _serial_number = codepack.get_serial_number()
            _state = None
            _codes = {k: v.get_serial_number() for k, v in codepack.codes.items()}
            _source = codepack.get_source()
            _structure = codepack.get_structure()
            _subscribe = codepack.subscribe
            _owner = codepack.owner
        else:
            _name = None
            _serial_number = None
            _state = None
            _codes = None
            _source = None
            _structure = None
            _subscribe = None
            _owner = None
        super().__init__(name=_name, serial_number=_serial_number, state=_state, timestamp=timestamp, owner=_owner)
        self.__setitem__('codes', _codes)
        self.__setitem__('source', _source)
        self.__setitem__('structure', _structure)
        self.__setitem__('subscribe', _subscribe)
        self.set_argpack(argpack=argpack)

    def set_argpack(self, argpack: Optional[Union[ArgPack, dict]] = None) -> None:
        if isinstance(argpack, ArgPack):
            tmp = argpack.to_dict()
        elif isinstance(argpack, dict):
            tmp = deepcopy(argpack)
            if '_id' not in tmp:
                tmp['_id'] = None
        elif argpack is None:
            tmp = {'_id': None}
        else:
            tmp = dict()
        self.__setitem__('argpack', tmp)
