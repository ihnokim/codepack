from codepack.storage import FileStorage
from codepack.service.storage_service import StorageService


class FileStorageService(StorageService):
    def __init__(self, item_type=None, path='./'):
        self.storage = FileStorage(item_type=item_type, key='id', path=path)

    def save(self, item, update=False):
        self.storage.save(item=item, update=update)

    def load(self, id):
        return self.storage.load(key=id)

    def remove(self, id):
        self.storage.remove(key=id)

    def check(self, id):
        return self.storage.exist(key=id)
