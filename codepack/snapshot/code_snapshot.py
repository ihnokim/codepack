from codepack.snapshot.snapshot import Snapshot
from codepack.dependency.dependency import Dependency


class CodeSnapshot(Snapshot):
    def __init__(self, code=None, args: tuple = None, kwargs: dict = None, timestamp: float = None, message: str = ''):
        if code:
            _id = code.id
            _serial_number = code.serial_number
            _state = code.get_state()
            _source = code.source
            _dependency = code.dependency
            _env = code.env
            _image = code.image
            _owner = code.owner
        else:
            _id = None
            _serial_number = None
            _state = None
            _source = None
            _dependency = None
            _env = None
            _image = None
            _owner = None
        super().__init__(id=_id, serial_number=_serial_number, state=_state, timestamp=timestamp,
                         env=_env, image=_image, owner=_owner, message=message)
        self.__setitem__('source', _source)
        self.set_args(args=args, kwargs=kwargs)
        self.set_dependency(dependency=_dependency)

    def set_args(self, args=None, kwargs=None):
        self.__setitem__('args', list(args) if args else list())
        self.__setitem__('kwargs', kwargs if kwargs else dict())

    def set_dependency(self, dependency):
        self.__setitem__('dependency', list())
        if dependency:
            for d in dependency.values():
                if isinstance(d, Dependency):
                    self.__getitem__('dependency').append(d.to_dict())
                elif isinstance(d, dict):
                    self.__getitem__('dependency').append(d)
                else:
                    raise TypeError(type(d))  # pragma: no cover
            self.__getitem__('dependency').sort(key=lambda x: x['serial_number'])

    @classmethod
    def from_dict(cls, d):
        ret = cls()
        for k, v in d.items():
            if k != '_id':
                ret[k] = v
        return ret
