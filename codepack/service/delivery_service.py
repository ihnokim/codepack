from codepack.service.mongodb_service import MongoDBService
from codepack.service.abc import DeliveryService
from codepack.utils.order import Order
from collections.abc import Iterable
from codepack.utils import Singleton
from datetime import datetime
import json
import os


class MemoryDeliveryService(DeliveryService, Singleton):
    def __init__(self):
        super().__init__()
        if not hasattr(self, 'deliveries'):
            self.deliveries = dict()

    def init(self):
        self.deliveries = dict()

    def send(self, sender, invoice_number, item=None, send_time=None):
        self.deliveries[invoice_number] = Order(sender=sender, invoice_number=invoice_number, item=item, send_time=send_time)

    def receive(self, invoice_number):
        return self.deliveries[invoice_number].receive()

    def check(self, invoice_number):
        if isinstance(invoice_number, str):
            if invoice_number in self.deliveries:
                d = self.deliveries[invoice_number].to_dict()
                d.pop('item', None)
                return d
            else:
                return None
        elif isinstance(invoice_number, Iterable):
            ret = list()
            for i in invoice_number:
                tmp = self.check(i)
                if tmp:
                    ret.append(tmp)
            return ret
        else:
            raise TypeError(type(invoice_number))  # pragma: no cover

    def cancel(self, invoice_number):
        self.deliveries.pop(invoice_number, None)


class FileDeliveryService(DeliveryService):
    def __init__(self, path='./'):
        super().__init__()
        self.path = path

    def send(self, sender, invoice_number, item=None, send_time=None, path=None):
        Order(sender=sender, invoice_number=invoice_number, item=item, send_time=send_time)\
            .to_file(path=Order.get_path(serial_number=invoice_number, path=path if path else self.path))

    def receive(self, invoice_number, path=None):
        return Order.from_file(path=Order.get_path(serial_number=invoice_number, path=path if path else self.path)).receive()

    def check(self, invoice_number, path=None):
        if isinstance(invoice_number, str):
            d = None
            try:
                d = Order.from_file(path=Order.get_path(serial_number=invoice_number, path=path if path else self.path)).to_dict()
                d.pop('item', None)
            finally:
                return d
        elif isinstance(invoice_number, Iterable):
            ret = list()
            for i in invoice_number:
                tmp = self.check(i)
                if tmp:
                    ret.append(tmp)
            return ret
        else:
            raise TypeError(type(invoice_number))  # pragma: no cover

    def cancel(self, invoice_number, path=None):
        os.remove(Order.get_path(serial_number=invoice_number, path=path if path else self.path))


class MongoDeliveryService(DeliveryService, MongoDBService):
    def __init__(self, db=None, collection=None,
                 mongodb=None, *args, **kwargs):
        MongoDBService.__init__(self, db=db, collection=collection,
                                mongodb=mongodb, *args, **kwargs)
        DeliveryService.__init__(self)

    def send(self, sender, invoice_number, item=None, send_time=None, db=None, collection=None):
        if not send_time:
            send_time = datetime.now().timestamp()
        d = {'sender': sender, 'send_time': send_time, 'item': json.dumps(item)}
        self.mongodb[db if db else self.db][collection if collection else self.collection]\
            .update_one({'_id': invoice_number}, {'$set': d}, upsert=True)

    def receive(self, invoice_number, db=None, collection=None):
        ret = self.mongodb[db if db else self.db][collection if collection else self.collection]\
            .find_one({'_id': invoice_number})
        return json.loads(ret['item'])

    def check(self, invoice_number, db=None, collection=None):
        if isinstance(invoice_number, str):
            return self.mongodb[db if db else self.db][collection if collection else self.collection]\
                .find_one({'_id': invoice_number}, projection={'item': 0})
        elif isinstance(invoice_number, Iterable):
            return list(self.mongodb[db if db else self.db][collection if collection else self.collection]
                        .find({'_id': {'$in': invoice_number}}, projection={'item': 0}))
        else:
            raise TypeError(type(invoice_number))  # pragma: no cover

    def cancel(self, invoice_number, db=None, collection=None):
        self.mongodb[db if db else self.db][collection if collection else self.collection]\
            .delete_one({'_id': invoice_number})
