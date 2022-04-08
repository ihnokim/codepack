from codepack.storages.storable import Storable
import json
from datetime import datetime
from typing import Any, Optional


class Delivery(Storable):
    def __init__(self, id: str, serial_number: str,
                 item: Optional[Any] = None, timestamp: Optional[float] = None) -> None:
        Storable.__init__(self, id=id, serial_number=serial_number)
        self.item = item
        self.timestamp = timestamp if timestamp else datetime.now().timestamp()

    def __str__(self) -> str:
        return '%s(id: %s, serial_number: %s)' % (self.__class__.__name__, self.id, self.serial_number)  # pragma: no cover

    def __repr__(self) -> str:
        return self.__str__()  # pragma: no cover

    def send(self, item: Any, timestamp: Optional[float] = None) -> None:
        timestamp if timestamp else datetime.now().timestamp()
        self.item = item

    def receive(self) -> Any:
        return self.item

    def to_dict(self) -> dict:
        return {'_id': self.serial_number, 'id': self.id, 'serial_number': self.serial_number,
                'timestamp': self.timestamp, 'item': json.dumps(self.item)}

    @classmethod
    def from_dict(cls, d: dict) -> 'Delivery':
        return cls(id=d['id'], serial_number=d['serial_number'],
                   item=json.loads(d['item']), timestamp=d.get('timestamp', None))
