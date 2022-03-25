from codepack.storage.storable import Storable
import json
from datetime import datetime


class Delivery(Storable):
    def __init__(self, id, serial_number, item=None, timestamp=None):
        Storable.__init__(self, id=id, serial_number=serial_number)
        self.item = item
        self.timestamp = timestamp if timestamp else datetime.now().timestamp()

    def __str__(self):
        return '%s(id: %s, serial_number: %s)' % (self.__class__.__name__, self.id, self.serial_number)  # pragma: no cover

    def __repr__(self):
        return self.__str__()  # pragma: no cover

    def send(self, item, timestamp=None):
        timestamp if timestamp else datetime.now().timestamp()
        self.item = item

    def receive(self):
        return self.item

    def to_dict(self):
        return {'_id': self.serial_number, 'id': self.id, 'serial_number': self.serial_number,
                'timestamp': self.timestamp, 'item': json.dumps(self.item)}

    @classmethod
    def from_dict(cls, d):
        return cls(id=d['id'], serial_number=d['serial_number'],
                   item=json.loads(d['item']), timestamp=d.get('timestamp', None))
