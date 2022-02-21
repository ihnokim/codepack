from codepack.storage import FileStorage
from codepack.service.delivery_service import DeliveryService


class FileDeliveryService(DeliveryService):
    def __init__(self, item_type=None, path='./'):
        self.storage = FileStorage(item_type=item_type, path=path)

    def send(self, id, serial_number, item=None, timestamp=None):
        self.storage.item_type(id=id, serial_number=serial_number, item=item, timestamp=timestamp)\
            .to_file(path=self.storage.item_type.get_path(key=serial_number, path=self.storage.path))

    def receive(self, serial_number):
        return self.storage.item_type.from_file(path=self.storage.item_type.get_path(key=serial_number, path=self.storage.path)).receive()

    def check(self, serial_number):
        return self.storage.exist(key=serial_number)

    def cancel(self, serial_number):
        self.storage.remove(key=serial_number)
