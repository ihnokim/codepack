from codepack.storage import MongoStorage
from codepack.service.storage_service import StorageService
from collections.abc import Iterable


class MongoStorageService(StorageService):
    def __init__(self, item_type=None, mongodb=None, db=None, collection=None, *args, **kwargs):
        self.storage = MongoStorage(item_type=item_type, mongodb=mongodb, db=db, collection=collection, *args, **kwargs)

    def save(self, item, update=False):
        if isinstance(item, self.storage.item_type):
            if update:
                d = item.to_dict()
                _id = d.pop('_id')
                self.storage.mongodb[self.storage.db][self.storage.collection]\
                    .update_one({'_id': _id}, {'$set': d}, upsert=True)
            elif self.check(item.id):
                raise ValueError('%s already exists' % item.id)
            else:
                item.to_db(mongodb=self.storage.mongodb, db=self.storage.db, collection=self.storage.collection)
        else:
            raise TypeError(type(item))

    def load(self, id):
        return self.storage.item_type\
            .from_db(id=id, mongodb=self.storage.mongodb, db=self.storage.db, collection=self.storage.collection)

    def remove(self, id):
        self.storage.remove(key=id)

    def check(self, id):
        if isinstance(id, str):
            ret = None
            item = self.storage.item_type.from_db(id=id,
                                                  mongodb=self.storage.mongodb,
                                                  db=self.storage.db, collection=self.storage.collection)
            if item:
                ret = item.id
            return ret
        elif isinstance(id, Iterable):
            ret = list()
            for item in self.storage.mongodb[self.storage.db][self.storage.collection]\
                    .find({'_id': {'$in': id}}, projection={}):
                ret.append(item['_id'])
            return ret
        else:
            raise TypeError(type(id))  # pragma: no cover
