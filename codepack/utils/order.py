from codepack.abc import Storable
import json
from datetime import datetime


class Order(Storable):
    def __init__(self, sender, invoice_number, item=None, send_time=None):
        super().__init__()
        self.sender = sender
        self.invoice_number = invoice_number
        self.item = item
        self.send_time = send_time if send_time else datetime.now().timestamp()

    def __str__(self):
        return '%s(sender: %s, invoice_number: %s)' % (self.__class__.__name__, self.sender, self.invoice_number)  # pragma: no cover

    def __repr__(self):
        return self.__str__()  # pragma: no cover

    def send(self, item):
        self.item = item

    def receive(self):
        return self.item

    def to_dict(self):
        return {'_id': self.invoice_number, 'sender': self.sender,
                'send_time': self.send_time, 'item': json.dumps(self.item)}

    @classmethod
    def from_dict(cls, d):
        return cls(sender=d['sender'], invoice_number=d['_id'], item=json.loads(d['item']), send_time=d.get('send_time', None))
