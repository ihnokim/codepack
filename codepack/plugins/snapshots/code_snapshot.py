from codepack.plugins.snapshots.snapshot import Snapshot
from codepack.plugins.dependency import Dependency
from codepack.plugins.callback import Callback
from typing import TypeVar, Optional


Code = TypeVar('Code', bound='codepack.code.Code')  # noqa: F821


class CodeSnapshot(Snapshot):
    def __init__(self, code: Optional[Code] = None, args: Optional[tuple] = None, kwargs: Optional[dict] = None,
                 timestamp: Optional[float] = None, message: str = '') -> None:
        if code:
            _name = code.get_name()
            _serial_number = code.get_serial_number()
            _state = code.get_state()
            _source = code.source
            _context = code.context
            _dependency = code.dependency
            _env = code.env
            _image = code.image
            _owner = code.owner
            _callback = code.callback
        else:
            _name = None
            _serial_number = None
            _state = None
            _source = None
            _context = None
            _dependency = None
            _env = None
            _image = None
            _owner = None
            _callback = None
        super().__init__(name=_name, serial_number=_serial_number, state=_state, timestamp=timestamp,
                         env=_env, image=_image, owner=_owner, message=message, context=_context)
        self.__setitem__('source', _source)
        self.set_args(args=args, kwargs=kwargs)
        self.set_dependency(dependency=_dependency)
        self.set_callback(callback=_callback)

    def set_args(self, args: Optional[tuple] = None, kwargs: Optional[dict] = None) -> None:
        self.__setitem__('args', list(args) if args else list())
        self.__setitem__('kwargs', kwargs if kwargs else dict())

    def set_dependency(self, dependency: Optional[dict]) -> None:
        self.__setitem__('dependency', list())
        if dependency:
            for d in dependency.values():
                if isinstance(d, Dependency):
                    self.__getitem__('dependency').append(d.to_dict())
                elif isinstance(d, dict):
                    self.__getitem__('dependency').append(d)
                else:
                    raise TypeError(type(d))  # pragma: no cover
            self.__getitem__('dependency').sort(key=lambda x: x['_serial_number'])

    def set_callback(self, callback: Optional[dict]) -> None:
        self.__setitem__('callback', list())
        if callback:
            for c in callback.values():
                if isinstance(c, Callback):
                    self.__getitem__('callback').append(c.to_dict())
                elif isinstance(c, dict):
                    self.__getitem__('callback').append(c)
                else:
                    raise TypeError(type(c))  # pragma: no cover
