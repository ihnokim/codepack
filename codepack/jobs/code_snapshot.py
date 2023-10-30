from typing import Any, Dict, Optional
import uuid
from datetime import datetime
from codepack.item import Item
from codepack.function import Function
from codepack.arg import Arg


class CodeSnapshot(Item):
    def __init__(self,
                 function: Function,
                 arg: Optional[Arg] = None,
                 serial_number: Optional[str] = None,
                 timestamp: Optional[float] = None,
                 name: Optional[str] = None,
                 ) -> None:
        self.serial_number: str = serial_number if serial_number else str(uuid.uuid4())
        self.timestamp: float = timestamp if timestamp else datetime.now().timestamp()
        self.function: Function = function
        self.arg: Optional[Arg] = arg
        super().__init__(name=name,
                         version=None,
                         owner=None,
                         description=None)

    def get_id(self) -> str:
        return self.serial_number

    def set_arg(self, arg: Optional[Arg]) -> None:
        self.arg = arg

    def __serialize__(self) -> Dict[str, Any]:
        return {'serial_number': self.serial_number,
                'name': self.name,
                'function': self.function.to_dict(),
                'arg': self.arg.to_dict() if self.arg else None,
                'timestamp': self.timestamp}

    @classmethod
    def __deserialize__(cls, d: Dict[str, Any]) -> 'CodeSnapshot':
        return cls(serial_number=d['serial_number'],
                   name=d['name'],
                   function=Function.from_dict(d['function']),
                   arg=Arg.from_dict(d['arg']) if d['arg'] else None,
                   timestamp=d['timestamp'])
