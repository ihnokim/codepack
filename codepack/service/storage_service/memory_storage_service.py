from codepack.storage import MemoryStorage
from codepack.service.storage_service import StorageService
from collections.abc import Iterable


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
        if isinstance(id, str):
            if id in self.storage.memory:
                return self.storage.memory[id].id
            else:
                return None
        elif isinstance(id, Iterable):
            ret = list()
            for i in id:
                tmp = self.check(i)
                if tmp:
                    ret.append(tmp)
            return ret
        else:
            raise TypeError(type(id))  # pragma: no cover
