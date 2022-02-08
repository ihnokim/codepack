from codepack.storage import Storage, Storable
from typing import Type


class MemoryStorage(Storage):
    def __init__(self, item_type: Type[Storable] = None):
        super().__init__(item_type=item_type)
        self.memory = None
        self.init()

    def init(self):
        self.memory = dict()

    def close(self):
        self.memory.clear()
        self.memory = None
