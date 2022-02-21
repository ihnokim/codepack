from codepack.storage import S3Storage
from codepack.service.delivery_service import DeliveryService
import json


class S3DeliveryService(DeliveryService):
    def __init__(self, item_type=None, s3=None, bucket=None, path=None, *args, **kwargs):
        self.storage = S3Storage(item_type=item_type, s3=s3, bucket=bucket, path=path, *args, **kwargs)

    def send(self, id, serial_number, item=None, timestamp=None):
        data = self.storage.item_type(id=id, serial_number=serial_number, item=item, timestamp=timestamp).to_json()
        path = self.storage.item_type.get_path(key=serial_number, path=self.storage.path)
        self.storage.s3.upload(bucket=self.storage.bucket, key=path, data=data)

    def receive(self, serial_number):
        path = self.storage.item_type.get_path(key=serial_number, path=self.storage.path)
        return json.loads(self.storage.s3.download(bucket=self.storage.bucket, key=path))

    def check(self, serial_number):
        return self.storage.exist(serial_number)

    def cancel(self, serial_number):
        self.storage.remove(key=serial_number)
