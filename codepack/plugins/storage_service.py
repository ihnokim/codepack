from codepack.plugins.service import Service
from typing import Union, TypeVar, Optional


Storage = TypeVar('Storage', bound='codepack.storages.storage.Storage')  # noqa: F821
Storable = TypeVar('Storable', bound='codepack.storages.storable.Storable')  # noqa: F821


class StorageService(Service):
    def __init__(self, storage: Storage) -> None:
        super().__init__(storage=storage)

    def save(self, item: Union[Storable, list], update: bool = False) -> None:
        self.storage.save(item=item, update=update)

    def load(self, id: Union[str, list]) -> Optional[Union[Storable, dict, list]]:
        return self.storage.load(key=id)

    def search(self, query: str, projection: Optional[list]) -> list:
        keys = self.storage.text_key_search(key=query)
        items = self.storage.load(key=keys, projection=projection, to_dict=True)
        return items

    def remove(self, id: Union[str, list]) -> None:
        self.storage.remove(key=id)

    def check(self, id: Union[str, list]) -> Union[bool, list]:
        return self.storage.exist(key=id)
