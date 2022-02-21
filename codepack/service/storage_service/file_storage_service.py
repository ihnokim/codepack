from codepack.storage import FileStorage
from codepack.service.storage_service import StorageService


class FileStorageService(StorageService):
    def __init__(self, item_type=None, path='./'):
        self.storage = FileStorage(item_type=item_type, path=path)

    def save(self, item, update=False):
        if isinstance(item, self.storage.item_type):
            if update:
                item.to_file(self.storage.item_type.get_path(key=item.id, path=self.storage.path))
            elif self.check(item.id):
                raise ValueError('%s already exists' % item.id)
            else:
                item.to_file(self.storage.item_type.get_path(key=item.id, path=self.storage.path))
        else:
            raise TypeError(type(item))

    def load(self, id):
        return self.storage.item_type.from_file(path=self.storage.item_type.get_path(key=id, path=self.storage.path))

    def remove(self, id):
        self.storage.remove(key=id)

    def check(self, id):
        return self.storage.exist(key=id)
