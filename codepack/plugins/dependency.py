from codepack.storages.storable import Storable
from typing import TypeVar, Optional, Union


Code = TypeVar('Code', bound='codepack.code.Code')  # noqa: F821


class Dependency(Storable):
    def __init__(self, code: Optional[Code] = None, name: Optional[str] = None, serial_number: Optional[str] = None,
                 param: Optional[str] = None) -> None:
        super().__init__(name=name, serial_number=serial_number)
        self.code = None
        self.param = None
        self.bind(code)
        self.depend_on(name=name, serial_number=serial_number, param=param)

    def __lshift__(self, sender: Union[Storable, dict]) -> None:
        if isinstance(sender, Storable):
            self.depend_on(name=sender.get_name(), serial_number=sender.get_serial_number(), param=self.param)
        elif isinstance(sender, dict):
            self.depend_on(name=sender['_name'], serial_number=sender['_serial_number'], param=self.param)
        else:
            raise TypeError(type(sender))
        self.code.add_dependency(self)

    def bind(self, code: Code) -> None:
        self.code = code

    def depend_on(self, name: Optional[str] = None, serial_number: Optional[str] = None,
                  param: Optional[str] = None) -> None:
        self.set_name(name=name)
        self.set_serial_number(serial_number=serial_number)
        self.param = param

    def __eq__(self, other: Union['Dependency', dict]) -> bool:
        if isinstance(other, type(self)):
            ret = True
            ret &= (self.get_serial_number() == other.get_serial_number())
            ret &= (self.get_name() == other.get_name())
            ret &= (self.param == other.param)
            return ret
        elif isinstance(other, dict):
            ret = True
            ret &= (self.get_serial_number() == other['_serial_number'])
            ret &= (self.get_name() == other['_name'])
            ret &= (self.param == other['param'])
            return ret
        else:
            return False

    def __str__(self) -> str:
        return '%s(param: %s, name: %s)' % (self.__class__.__name__, self.param, self.get_name())  # pragma: no cover

    def __repr__(self) -> str:
        return self.__str__()  # pragma: no cover

    def to_dict(self) -> dict:
        ret = self.get_meta()
        ret.pop('_timestamp', None)
        ret['param'] = self.param
        return ret

    @classmethod
    def from_dict(cls, d: dict) -> 'Dependency':
        return cls(name=d['_name'], serial_number=d['_serial_number'], param=d['param'])
