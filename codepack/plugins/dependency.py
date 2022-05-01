from codepack.storages.storable import Storable
from typing import TypeVar, Optional, Union


Code = TypeVar('Code', bound='codepack.code.Code')


class Dependency(Storable):
    def __init__(self, code: Optional[Code] = None, id: Optional[str] = None, serial_number: Optional[str] = None,
                 param: Optional[str] = None) -> None:
        super().__init__(id=id, serial_number=serial_number)
        self.code = None
        self.param = None
        self.bind(code)
        self.depend_on(id=id, serial_number=serial_number, param=param)

    def __lshift__(self, sender: Union[Storable, dict]) -> None:
        if isinstance(sender, Storable):
            self.depend_on(id=sender.id, serial_number=sender.serial_number, param=self.param)
        elif isinstance(sender, dict):
            self.depend_on(id=sender['id'], serial_number=sender['serial_number'], param=self.param)
        else:
            raise TypeError(type(sender))
        self.code.add_dependency(self)

    def bind(self, code: Code) -> None:
        self.code = code

    def depend_on(self, id: Optional[str] = None, serial_number: Optional[str] = None,
                  param: Optional[str] = None) -> None:
        self.id = id
        self.serial_number = serial_number
        self.param = param

    def __eq__(self, other: Union['Dependency', dict]) -> bool:
        if isinstance(other, type(self)):
            ret = True
            ret &= (self.serial_number == other.serial_number)
            ret &= (self.id == other.id)
            ret &= (self.param == other.param)
            return ret
        elif isinstance(other, dict):
            ret = True
            ret &= (self.serial_number == other['serial_number'])
            ret &= (self.id == other['id'])
            ret &= (self.param == other['param'])
            return ret
        else:
            return False

    def __str__(self) -> str:
        return '%s(param: %s, id: %s)' % (self.__class__.__name__, self.param, self.id)  # pragma: no cover

    def __repr__(self) -> str:
        return self.__str__()  # pragma: no cover

    def to_dict(self) -> dict:
        return {'id': self.id, 'serial_number': self.serial_number, 'param': self.param}

    @classmethod
    def from_dict(cls, d: dict) -> 'Dependency':
        return cls(id=d['id'], serial_number=d['serial_number'], param=d['param'])
