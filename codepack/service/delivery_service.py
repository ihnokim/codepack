from codepack.service.service import Service
from typing import Union


class DeliveryService(Service):
    def __init__(self, storage):
        super().__init__(storage=storage)
        if self.storage.key != 'serial_number':
            self.storage.key = 'serial_number'

    def send(self, id: str, serial_number: str, item: object = None, timestamp: float = None):
        item = self.storage.item_type(id=id, serial_number=serial_number, item=item, timestamp=timestamp)
        self.storage.save(item=item, update=True)

    def receive(self, serial_number: str):
        return self.storage.load(key=serial_number).receive()

    def check(self, serial_number: Union[str, list]):
        return self.storage.exist(key=serial_number, summary='and')

    def cancel(self, serial_number: Union[str, list]):
        self.storage.remove(key=serial_number)
