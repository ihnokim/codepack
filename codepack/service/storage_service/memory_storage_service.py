from codepack.storage import MemoryStorage
from codepack.service.storage_service import StorageService
from collections.abc import Iterable


class MemoryStorageService(StorageService, MemoryStorage):
    def __init__(self, obj=None):
        MemoryStorage.__init__(self, obj=obj)

    def save(self, item, update=False):
        if isinstance(item, self.obj):
            if update:
                self.memory[item.id] = item
            elif self.check(item.id):
                raise ValueError('%s already exists' % item.id)
            else:
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
