import abc
from codepack.storage import Storage


class Service(metaclass=abc.ABCMeta):
    def __init__(self, storage: Storage):
        self.storage = storage

    def close(self):
        self.storage.close()
