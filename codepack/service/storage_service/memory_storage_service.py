from codepack.storage import MemoryStorage
from codepack.service.storage_service import StorageService


class MemoryStorageService(StorageService):
    def __init__(self, item_type=None):
        self.storage = MemoryStorage(item_type=item_type)

    def save(self, item, update=False):
        if isinstance(item, self.storage.item_type):
            if update:
                self.storage.memory[item.id] = item
            elif self.check(item.id):
                raise ValueError('%s already exists' % item.id)
            else:
                self.storage.memory[item.id] = item
        else:
            raise TypeError(type(item))

    def load(self, id):
        return self.storage.memory[id]

    def remove(self, id):
        self.storage.remove(key=id)

    def check(self, id):
        return self.storage.exist(key=id)
