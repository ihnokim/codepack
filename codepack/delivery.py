from codepack.abc import CodeBase, MongoDBService
from collections.abc import Iterable


class Delivery:
    def __init__(self, name=None, item=None, sender=None):
        self.name = name
        self.item = item
        self.sender = sender

    def cancel(self):
        self.item = None
        self.sender = None

    def __lshift__(self, sender):
        if isinstance(sender, str):
            self.sender = sender
            self.item = None
        elif isinstance(sender, CodeBase):
            self.sender = sender.id
            self.item = None
        else:
            raise TypeError(type(sender))

    def send(self, item):
        self.item = item

    def __str__(self):
        return '%s(name: %s, sender: %s)' % (self.__class__.__name__, self.name, self.sender)

    def __repr__(self):
        return self.__str__()


class DeliveryService(Iterable, MongoDBService):
    def __init__(self, deliveries=None, db=None, collection=None,
                 config=None, ssh_config=None, mongodb=None, offline=False, **kwargs):
        super().__init__(db=db, collection=collection,
                         config=config, ssh_config=ssh_config, mongodb=mongodb, offline=offline, **kwargs)
        if deliveries is None:
            self.deliveries = dict()
        elif isinstance(deliveries, dict):
            self.deliveries = deliveries
        else:
            raise TypeError(type(deliveries))

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

    def cancel_deliveries(self, sender=None):
        for delivery in self:
            if sender is None or delivery.sender == sender:
                delivery.cancel()

    def send_deliveries(self, sender=None, item=None):
        for delivery in self:
            if sender is None or delivery.sender == sender:
                delivery.send(item)

    def return_deliveries(self, sender=None):
        self.send_deliveries(sender=sender, item=None)

    def inquire(self, name):
        return self.__getitem__(name)

    def request(self, name, sender=None):
        self.__setitem__(name, Delivery(name=name, sender=sender))

    def get_senders(self):
        return {k: v.sender for k, v in self.items() if v.sender is not None}
