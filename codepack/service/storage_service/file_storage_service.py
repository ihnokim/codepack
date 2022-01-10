import os
from codepack.storage import FileStorage
from codepack.service.storage_service import StorageService
from collections.abc import Iterable


class FileStorageService(StorageService, FileStorage):
    def __init__(self, obj=None, path='./'):
        FileStorage.__init__(self, obj=obj, path=path)

    def save(self, item):
        if isinstance(item, self.obj):
            item.to_file(self.obj.get_path(serial_number=item.id, path=self.path))
        else:
            raise TypeError(type(item))

    def load(self, id):
        return self.obj.from_file(path=self.obj.get_path(serial_number=id, path=self.path))

    def remove(self, id):
        os.remove(path=self.obj.get_path(serial_number=id, path=self.path))

    def check(self, id):
        if isinstance(id, str):
            ret = None
            try:
                ret = self.obj.from_file(path=self.obj.get_path(serial_number=id, path=self.path)).id
            finally:
                return ret
        elif isinstance(id, Iterable):
            ret = list()
            for i in id:
                tmp = self.check(i)
                if tmp:
                    ret.append(tmp)
            return ret
        else:
            raise TypeError(type(id))  # pragma: no cover
