from codepack.storage import S3Storage
from codepack.service.delivery_service import DeliveryService
from codepack.delivery import Delivery
from typing import Type


class S3DeliveryService(DeliveryService):
    def __init__(self, item_type: Type[Delivery] = None, s3=None, bucket=None, path=None, *args, **kwargs):
        self.storage = S3Storage(item_type=item_type, key='serial_number',
                                 s3=s3, bucket=bucket, path=path, *args, **kwargs)

    def send(self, id: str, serial_number: str, item: object = None, timestamp: float = None):
        item = self.storage.item_type(id=id, serial_number=serial_number, item=item, timestamp=timestamp)
        self.storage.save(item=item, update=True)

    def receive(self, serial_number: str):
        return self.storage.load(key=serial_number).receive()

    def check(self, serial_number: str):
        return self.storage.exist(key=serial_number)

    def cancel(self, serial_number: str):
        self.storage.remove(key=serial_number)
