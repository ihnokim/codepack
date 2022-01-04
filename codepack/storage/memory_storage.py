from codepack.storage import Storage


class MemoryStorage(Storage):
    def __init__(self, obj=None):
        super().__init__(obj=obj)
        self.memory = None
        self.init()

    def init(self):
        self.memory = dict()
