from codepack.service.service import Service
from codepack.callback.callback import Callback
from collections.abc import Callable


class CallbackService(Service):
    def __init__(self, storage):
        super().__init__(storage=storage)
        if self.storage.key != 'id':
            self.storage.key = 'id'

    def push(self, callback: Callable, context: dict = None):
        cb = Callback(function=callback, context=context)
        self.storage.save(cb, update=True)
        return cb.id

    def pull(self, name: str):
        cb = self.storage.load(key=name)
        return cb

    def remove(self, name: str):
        self.storage.remove(key=name)

    def exist(self, name: str):
        return self.storage.exist(key=name)
