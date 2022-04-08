import abc
from typing import TypeVar


Storage = TypeVar('Storage', bound='codepack.storages.storage.Storage')


class Service(metaclass=abc.ABCMeta):
    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    def close(self) -> None:
        self.storage.close()
