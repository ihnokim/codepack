import inspect
from copy import copy
from collections.abc import Iterable, Callable
import dill
import bson
from codepack.status import Status
from codepack.delivery import Delivery, DeliveryService
from codepack.interface import MongoDB
from codepack.abc import AbstractCode


class Code(AbstractCode):
    def __init__(self, function, id=None):
        super().__init__()

        self.status = None

        self.function = None
        self.source = None
        self.description = None

        self.parents = None
        self.children = None
        self.delivery_service = None

        self.set_function(function)
        if id is None:
            self.id = function.__name__
        else:
            self.id = id

        self.init()

    @staticmethod
    def getsource(function):
        assert isinstance(function, Callable), "'function' should be an instance of Callable."
        assert function.__name__ != '<lambda>', "Invalid function '<lambda>'."
        source = None

        for test in [inspect.getsource, dill.source.getsource]:
            try:
                source = test(function)
            except Exception:
                pass
            if source is not None:
                break

        return source

    def set_function(self, function):
        self.function = function
        self.source = self.getsource(self.function)
        self.description = self.function.__doc__.strip() if self.function.__doc__ is not None else str()

    def init(self):
        self.parents = dict()
        self.children = dict()
        self.delivery_service = DeliveryService()
        for arg in inspect.getfullargspec(self.function).args:
            self.delivery_service.request(arg)
        self.update_status(Status.NEW)

    def get_ready(self):
        self.update_status(Status.READY)
        self.delivery_service.return_deliveries()

    @staticmethod
    def return_delivery(sender, delivery):
        if isinstance(delivery, Iterable):
            for d in delivery:
                if d.sender == sender:
                    d.send(None)
        elif isinstance(delivery, Delivery):
            if delivery.sender == sender:
                delivery.send(None)
        else:
            raise TypeError(type(delivery))

    def receive(self, arg):
        return self.delivery_service.inquire(arg)

    def __rshift__(self, other):
        if isinstance(other, self.__class__):
            self.children[other.id] = other
            other.parents[self.id] = self

            other.delivery_service.return_deliveries(sender=self.id)
            self.get_ready()
        elif isinstance(other, Iterable):
            for t in other:
                self.__rshift__(t)
        else:
            raise TypeError(type(other))

    def __str__(self):
        return '%s(id: %s, function: %s, args: %s, receive: %s, status: %s)' % \
               (self.__class__.__name__, self.id, self.function.__name__,
                inspect.getfullargspec(self.function).args,
                self.delivery_service.get_senders(),
                self.status)

    def __repr__(self):
        return self.__str__()

    def update_status(self, status):
        self.status = status

    def __call__(self, *args, **kwargs):
        self.update_status(Status.RUNNING)

        for delivery in self.delivery_service:
            if delivery.sender is not None and delivery.name not in kwargs:
                kwargs[delivery.name] = delivery.item

        ret = self.function(*args, **kwargs)

        for c in self.children.values():
            c.delivery_service.send_deliveries(sender=self.id, item=ret)

        self.update_status(Status.TERMINATED)
        return ret

    def clone(self):
        tmp = copy(self)
        tmp.init()
        return tmp

    def save(self, filename):
        dill.dump(self, open(filename, 'wb'))

    @staticmethod
    def load(filename):
        return dill.load(open(filename, 'rb'))

    def to_binary(self):
        return bson.Binary(dill.dumps(self))

    @staticmethod
    def from_binary(b):
        return dill.loads(b)

    def to_db(self, db, collection, config):
        tmp = self.clone()
        mc = MongoDB(config)
        document = dict()
        document['_id'] = tmp.id
        document['binary'] = tmp.to_binary()
        document['summary'] = tmp.__str__()
        document['source'] = tmp.source
        document['description'] = tmp.description
        mc.client[db][collection].insert_one(document)
        mc.close()

    @staticmethod
    def from_db(id, db, collection, config):
        mc = MongoDB(config)
        ret = mc[db][collection].find_one({'_id': id})
        mc.close()
        if ret is None:
            return ret
        else:
            return Code.from_binary(ret['binary'])
