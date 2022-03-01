from codepack.storage import MemoryStorage
from codepack.service.delivery_service import DeliveryService
from codepack.delivery import Delivery
from typing import Type


class MemoryDeliveryService(DeliveryService):
    def __init__(self, item_type: Type[Delivery] = None):
        self.storage = MemoryStorage(item_type=item_type, key='serial_number')

    def send(self, id: str, serial_number: str, item: object = None, timestamp: float = None):
        item = self.storage.item_type(id=id, serial_number=serial_number, item=item, timestamp=timestamp)
        self.storage.save(item=item, update=True)

    def receive(self, serial_number: str):
        return self.storage.load(key=serial_number).receive()

    def check(self, serial_number: str):
        return self.storage.exist(key=serial_number, summary='and')

    def cancel(self, serial_number: str):
        self.storage.remove(key=serial_number)
