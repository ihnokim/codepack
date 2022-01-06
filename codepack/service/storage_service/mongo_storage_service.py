from codepack.storage import MongoStorage
from codepack.service.storage_service import StorageService
from collections.abc import Iterable


class MongoStorageService(StorageService, MongoStorage):
    def __init__(self, obj=None, mongodb=None, db=None, collection=None, *args, **kwargs):
        MongoStorage.__init__(self, obj=obj, mongodb=mongodb, db=db, collection=collection, *args, **kwargs)

    def save(self, item):
        if isinstance(item, self.obj):
            item.to_db(mongodb=self.mongodb, db=self.db, collection=self.collection)
        else:
            raise TypeError(type(item))

    def load(self, id):
        return self.obj.from_db(id=id, mongodb=self.mongodb, db=self.db, collection=self.collection)

    def remove(self, id):
        self.mongodb[self.db][self.collection].delete_one({'_id': id})

    def check(self, id):
        if isinstance(id, str):
            ret = None
            item = self.obj.from_db(id=id, mongodb=self.mongodb, db=self.db, collection=self.collection)
            if item:
                ret = item.id
            return ret
        elif isinstance(id, Iterable):
            ret = list()
            for item in self.mongodb[self.db][self.collection].find({'_id': {'$in': id}}, projection={}):
                ret.append(item['_id'])
            return ret
        else:
            raise TypeError(type(id))  # pragma: no cover
