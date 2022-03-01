from codepack.service.service import Service
from typing import Union


class StorageService(Service):
    def __init__(self, storage):
        super().__init__(storage=storage)
        if self.storage.key != 'id':
            self.storage.key = 'id'

    def save(self, item, update: bool = False):
        self.storage.save(item=item, update=update)

    def load(self, id: Union[str, list]):
        return self.storage.load(key=id)

    def remove(self, id: Union[str, list]):
        self.storage.remove(key=id)

    def check(self, id: Union[str, list]):
        return self.storage.exist(key=id)
