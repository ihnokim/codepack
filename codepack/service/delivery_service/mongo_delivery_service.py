from codepack.storage import MongoStorage
from codepack.service.delivery_service import DeliveryService
import json


class MongoDeliveryService(DeliveryService):
    def __init__(self, item_type=None, mongodb=None, db=None, collection=None, *args, **kwargs):
        self.storage = MongoStorage(item_type=item_type, mongodb=mongodb, db=db, collection=collection, *args, **kwargs)

    def send(self, id, serial_number, item=None, timestamp=None):
        d = self.storage.item_type(id=id, serial_number=serial_number, item=item, timestamp=timestamp).to_dict()
        d.pop('_id', None)
        self.storage.mongodb[self.storage.db][self.storage.collection]\
            .update_one({'_id': serial_number}, {'$set': d}, upsert=True)

    def receive(self, serial_number):
        ret = self.storage.mongodb[self.storage.db][self.storage.collection].find_one({'_id': serial_number})
        return json.loads(ret['item'])

    def check(self, serial_number):
        return self.storage.exist(key=serial_number)

    def cancel(self, serial_number):
        self.storage.remove(key=serial_number)
