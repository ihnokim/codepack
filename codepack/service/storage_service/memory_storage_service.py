from codepack.storage import MemoryStorage
from codepack.service.storage_service import StorageService
from collections.abc import Iterable


class MemoryStorageService(StorageService, MemoryStorage):
    def __init__(self, obj=None):
        MemoryStorage.__init__(self, obj=obj)

    def save(self, item):
        if isinstance(item, self.obj):
            self.memory[item.id] = item
        else:
            raise TypeError(type(item))

    def load(self, id):
        return self.memory[id]

    def remove(self, id):
        self.memory.pop(id, None)

    def check(self, id):
        if isinstance(id, str):
            if id in self.memory:
                return self.memory[id].id
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
