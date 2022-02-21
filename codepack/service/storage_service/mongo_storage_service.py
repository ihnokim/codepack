from codepack.storage import MongoStorage
from codepack.service.storage_service import StorageService


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
        return self.storage.exist(key=id)
