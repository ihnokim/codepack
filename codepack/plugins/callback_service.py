from codepack.plugins.service import Service
from codepack.plugins.callback import Callback
from collections.abc import Callable
from typing import TypeVar, Union, Optional


Storage = TypeVar('Storage', bound='codepack.storages.storage.Storage')
Storable = TypeVar('Storable', bound='codepack.storages.storable.Storable')


class CallbackService(Service):
    def __init__(self, storage: Storage) -> None:
        super().__init__(storage=storage)
        if self.storage.key != 'id':
            self.storage.key = 'id'

    def push(self, callback: Callable, context: Optional[dict] = None) -> str:
        cb = Callback(function=callback, context=context)
        self.storage.save(cb, update=True)
        return cb.id

    def pull(self, name: Union[str, list]) -> Optional[Union[Storable, dict, list]]:
        cb = self.storage.load(key=name)
        return cb

    def remove(self, name: Union[str, list]) -> None:
        self.storage.remove(key=name)

    def exist(self, name: Union[str, list]) -> Union[bool, list]:
        return self.storage.exist(key=name)
