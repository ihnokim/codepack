from codepack.storages.storable import Storable
import json
from datetime import datetime
from typing import Any, Optional


class Delivery(Storable):
    def __init__(self, name: str, serial_number: str,
                 item: Optional[Any] = None, timestamp: Optional[float] = None) -> None:
        Storable.__init__(self, name=name, serial_number=serial_number, timestamp=timestamp, id_key='_serial_number')
        self.item = item

    def __str__(self) -> str:
        return '%s(name: %s, serial_number: %s)' % \
               (self.__class__.__name__, self.get_name(), self.get_serial_number())  # pragma: no cover

    def __repr__(self) -> str:
        return self.__str__()  # pragma: no cover

    def send(self, item: Any, timestamp: Optional[float] = None) -> None:
        timestamp if timestamp else datetime.now().timestamp()
        self.item = item

    def receive(self) -> Any:
        return self.item

    def to_dict(self) -> dict:
        ret = self.get_metadata()
        ret['item'] = json.dumps(self.item)
        return ret

    @classmethod
    def from_dict(cls, d: dict) -> 'Delivery':
        return cls(name=d['_name'], serial_number=d['_serial_number'],
                   item=json.loads(d['item']), timestamp=d.get('_timestamp', None))
