from codepack.storage import MongoStorage
from codepack.service.delivery_service import DeliveryService
from collections.abc import Iterable
import json


class MongoDeliveryService(DeliveryService, MongoStorage):
    def __init__(self, obj=None, db=None, collection=None, mongodb=None, *args, **kwargs):
        MongoStorage.__init__(self, obj=obj, db=db, collection=collection, mongodb=mongodb, *args, **kwargs)

    def send(self, id, serial_number, item=None, timestamp=None):
        d = self.obj(id=id, serial_number=serial_number, item=item, timestamp=timestamp).to_dict()
        d.pop('_id', None)
        self.mongodb[self.db][self.collection].update_one({'_id': serial_number}, {'$set': d}, upsert=True)

    def receive(self, serial_number):
        ret = self.mongodb[self.db][self.collection].find_one({'_id': serial_number})
        return json.loads(ret['item'])

    def check(self, serial_number):
        if isinstance(serial_number, str):
            return self.mongodb[self.db][self.collection].find_one({'_id': serial_number}, projection={'item': 0})
        elif isinstance(serial_number, Iterable):
            return list(self.mongodb[self.db][self.collection].find({'_id': {'$in': serial_number}}, projection={'item': 0}))
        else:
            raise TypeError(type(serial_number))  # pragma: no cover

    def cancel(self, serial_number):
        self.mongodb[self.db][self.collection].delete_one({'_id': serial_number})
