from codepack.storage.storage import Storage
import abc


class Service(metaclass=abc.ABCMeta):
    def __init__(self, storage: Storage):
        self.storage = storage

    def close(self):
        self.storage.close()
