from collections.abc import Callable
from codepack.service.service import Service
from codepack.callback import Callback


class CallbackService(Service):
    def __init__(self, storage):
        super().__init__(storage=storage)
        if self.storage.key != 'id':
            self.storage.key = 'id'

    def push(self, callback: Callable, context: dict = None):
        cb = Callback(function=callback, context=context)
        self.storage.save(cb)

    def pull(self, name: str):
        cb = self.storage.load(key=name)
        return cb
