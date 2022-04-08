from codepack.plugins.service import Service
from typing import Union, TypeVar, Optional


Storage = TypeVar('Storage', bound='codepack.storages.storage.Storage')
Storable = TypeVar('Storable', bound='codepack.storages.storable.Storable')


class StorageService(Service):
    def __init__(self, storage: Storage) -> None:
        super().__init__(storage=storage)
        if self.storage.key != 'id':
            self.storage.key = 'id'

    def save(self, item: Union[Storable, list], update: bool = False) -> None:
        self.storage.save(item=item, update=update)

    def load(self, id: Union[str, list]) -> Optional[Union[Storable, dict, list]]:
        return self.storage.load(key=id)

    def remove(self, id: Union[str, list]) -> None:
        self.storage.remove(key=id)

    def check(self, id: Union[str, list]) -> Union[bool, list]:
        return self.storage.exist(key=id)
