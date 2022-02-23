from codepack.storage import MongoStorage
from codepack.service.storage_service import StorageService


class MongoStorageService(StorageService):
    def __init__(self, item_type=None, mongodb=None, db=None, collection=None, *args, **kwargs):
        self.storage = MongoStorage(item_type=item_type, key='id',
                                    mongodb=mongodb, db=db, collection=collection, *args, **kwargs)

    def save(self, item, update=False):
        self.storage.save(item=item, update=update)

    def load(self, id):
        return self.storage.load(key=id)

    def remove(self, id):
        self.storage.remove(key=id)

    def check(self, id):
        return self.storage.exist(key=id)
