from codepack.storages.storable import Storable
from typing import TypeVar, Optional, Union


Code = TypeVar('Code', bound='codepack.code.Code')


class Dependency(Storable):
    def __init__(self, code: Optional[Code] = None, id: Optional[str] = None, serial_number: Optional[str] = None,
                 arg: Optional[str] = None) -> None:
        super().__init__(id=id, serial_number=serial_number)
        self.code = None
        self.arg = None
        self.bind(code)
        self.depend_on(id=id, serial_number=serial_number, arg=arg)

    def __lshift__(self, sender: Union[Storable, dict]) -> None:
        if isinstance(sender, Storable):
            self.depend_on(id=sender.id, serial_number=sender.serial_number, arg=self.arg)
        elif isinstance(sender, dict):
            self.depend_on(id=sender['id'], serial_number=sender['serial_number'], arg=self.arg)
        else:
            raise TypeError(type(sender))
        self.code.add_dependency(self)

    def bind(self, code: Code) -> None:
        self.code = code

    def depend_on(self, id: Optional[str] = None, serial_number: Optional[str] = None,
                  arg: Optional[str] = None) -> None:
        self.id = id
        self.serial_number = serial_number
        self.arg = arg

    def __eq__(self, other: Union['Dependency', dict]) -> bool:
        if isinstance(other, type(self)):
            ret = True
            ret &= (self.serial_number == other.serial_number)
            ret &= (self.id == other.id)
            ret &= (self.arg == other.arg)
            return ret
        elif isinstance(other, dict):
            ret = True
            ret &= (self.serial_number == other['serial_number'])
            ret &= (self.id == other['id'])
            ret &= (self.arg == other['arg'])
            return ret
        else:
            return False

    def __str__(self) -> str:
        return '%s(arg: %s, id: %s)' % (self.__class__.__name__, self.arg, self.id)  # pragma: no cover

    def __repr__(self) -> str:
        return self.__str__()  # pragma: no cover

    def to_dict(self) -> dict:
        return {'id': self.id, 'serial_number': self.serial_number, 'arg': self.arg}

    @classmethod
    def from_dict(cls, d: dict) -> 'Dependency':
        return cls(id=d['id'], serial_number=d['serial_number'], arg=d['arg'])
