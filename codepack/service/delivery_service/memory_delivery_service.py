from codepack.storage import MemoryStorage
from codepack.service.delivery_service import DeliveryService
from collections.abc import Iterable


class MemoryDeliveryService(DeliveryService, MemoryStorage):
    def __init__(self, obj=None):
        MemoryStorage.__init__(self, obj=obj)

    def send(self, id, serial_number, item=None, timestamp=None):
        self.memory[serial_number] = self.obj(id=id, serial_number=serial_number, item=item, timestamp=timestamp)

    def receive(self, serial_number):
        return self.memory[serial_number].receive()

    def check(self, serial_number):
        if isinstance(serial_number, str):
            if serial_number in self.memory:
                d = self.memory[serial_number].to_dict()
                d.pop('item', None)
                return d
            else:
                return None
        elif isinstance(serial_number, Iterable):
            ret = list()
            for i in serial_number:
                tmp = self.check(i)
                if tmp:
                    ret.append(tmp)
            return ret
        else:
            raise TypeError(type(serial_number))  # pragma: no cover

    def cancel(self, serial_number):
        self.memory.pop(serial_number, None)
