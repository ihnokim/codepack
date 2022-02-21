from codepack.storage import MemoryStorage
from codepack.service.delivery_service import DeliveryService


class MemoryDeliveryService(DeliveryService, MemoryStorage):
    def __init__(self, item_type=None):
        MemoryStorage.__init__(self, item_type=item_type)

    def send(self, id, serial_number, item=None, timestamp=None):
        self.memory[serial_number] = self.item_type(id=id, serial_number=serial_number, item=item, timestamp=timestamp)

    def receive(self, serial_number):
        return self.memory[serial_number].receive()

    def cancel(self, serial_number):
        self.memory.pop(serial_number, None)

    def check(self, serial_number):
        return self.exist(key=serial_number)
