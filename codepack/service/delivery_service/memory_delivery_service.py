from codepack.storage import MemoryStorage
from codepack.service.delivery_service import DeliveryService


class MemoryDeliveryService(DeliveryService):
    def __init__(self, item_type=None):
        self.storage = MemoryStorage(item_type=item_type)

    def send(self, id, serial_number, item=None, timestamp=None):
        self.storage.memory[serial_number] =\
            self.storage.item_type(id=id, serial_number=serial_number, item=item, timestamp=timestamp)

    def receive(self, serial_number):
        return self.storage.memory[serial_number].receive()

    def check(self, serial_number):
        return self.storage.exist(key=serial_number, summary='and')

    def cancel(self, serial_number):
        self.storage.remove(key=serial_number)
