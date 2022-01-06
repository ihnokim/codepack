import os
from codepack.storage import FileStorage
from codepack.service.delivery_service import DeliveryService
from collections.abc import Iterable


class FileDeliveryService(DeliveryService, FileStorage):
    def __init__(self, obj=None, path='./'):
        FileStorage.__init__(self, obj=obj, path=path)

    def send(self, id, serial_number, item=None, timestamp=None):
        self.obj(id=id, serial_number=serial_number, item=item, timestamp=timestamp)\
            .to_file(path=self.obj.get_path(serial_number=serial_number, path=self.path))

    def receive(self, serial_number):
        return self.obj.from_file(path=self.obj.get_path(serial_number=serial_number, path=self.path)).receive()

    def check(self, serial_number):
        if isinstance(serial_number, str):
            d = None
            try:
                d = self.obj.from_file(path=self.obj.get_path(serial_number=serial_number, path=self.path)).to_dict()
                d.pop('item', None)
            finally:
                return d
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
        os.remove(self.obj.get_path(serial_number=serial_number, path=self.path))
