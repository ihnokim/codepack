from codepack.abc import CodeBase, MongoDBService
from collections.abc import Iterable
from datetime import datetime
import json


class Order:
    def __init__(self, name):
        self.name = name
        self.invoice_number = None
        self.sender = None

    def cancel(self):
        self.sender = None

    def __lshift__(self, sender):
        if isinstance(sender, CodeBase):
            self.invoice_number = sender.serial_number
            self.sender = sender.id
        else:
            raise TypeError(type(sender))

    def __str__(self):
        return '%s(name: %s, sender: %s)' % (self.__class__.__name__, self.name, self.sender)

    def __repr__(self):
        return self.__str__()


class DeliveryService(Iterable, MongoDBService):
    def __init__(self, deliveries=None, db=None, collection=None,
                 mongodb=None, online=False, **kwargs):
        super().__init__(db=db, collection=collection,
                         mongodb=mongodb, online=online, **kwargs)
        if deliveries is None:
            self.deliveries = dict()
        elif isinstance(deliveries, dict):
            self.deliveries = deliveries
        else:
            raise TypeError(type(deliveries))
        self.tmp_storage = None

    def __iter__(self):
        return iter(self.deliveries.values())

    def __setitem__(self, key, value):
        self.deliveries[key] = value

    def __getitem__(self, key):
        return self.deliveries[key]

    def __str__(self):
        return '%s(%s)' % (self.__class__.__name__, list(self.deliveries.values()))

    def __repr__(self):
        return self.__str__()

    def keys(self):
        return self.deliveries.keys()

    def values(self):
        return self.deliveries.values()

    def items(self):
        return self.deliveries.items()

    def send(self, sender, item=None):
        if self.online:
            if not isinstance(item, str):
                item = json.dumps(item)
            d = {'sender': sender.id,
                 'item': item, 'arrival_time': datetime.now()}
            self.mongodb[self.db][self.collection].update_one({'_id': sender.serial_number}, {'$set': d}, upsert=True)
        else:
            self.tmp_storage = item

    def receive(self, invoice_number):
        if self.online:
            ret = self.mongodb[self.db][self.collection].find_one({'_id': invoice_number})
            return json.loads(ret['item'])
        else:
            raise ConnectionError("cannot find DB client")

    def get_order(self, name):
        return self.__getitem__(name)

    def order(self, name):
        self.__setitem__(name, Order(name=name))

    def get_senders(self):
        return {k: v.sender for k, v in self.items() if v.sender is not None}
