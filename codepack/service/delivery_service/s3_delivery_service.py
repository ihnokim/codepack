from codepack.storage import S3Storage
from codepack.service.delivery_service import DeliveryService
import json


class S3DeliveryService(DeliveryService, S3Storage):
    def __init__(self, item_type=None, s3=None, bucket=None, path=None, *args, **kwargs):
        S3Storage.__init__(self, item_type=item_type, s3=s3, bucket=bucket, path=path, *args, **kwargs)

    def send(self, id, serial_number, item=None, timestamp=None):
        data = self.item_type(id=id, serial_number=serial_number, item=item, timestamp=timestamp).to_json()
        path = self.item_type.get_path(key=serial_number, path=self.path)
        self.s3.upload(bucket=self.bucket, key=path, data=data)

    def receive(self, serial_number):
        path = self.item_type.get_path(key=serial_number, path=self.path)
        return json.loads(self.s3.download(bucket=self.bucket, key=path))

    def check(self, serial_number):
        return self.exist(serial_number)

    def cancel(self, serial_number):
        path = self.item_type.get_path(key=serial_number, path=self.path)
        self.s3.delete(bucket=self.bucket, key=path)
