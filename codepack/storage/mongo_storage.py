from codepack.interface import MongoDB
from codepack.storage import Storage, Storable
from typing import Type, Union


class MongoStorage(Storage):
    def __init__(self, item_type: Type[Storable] = None,
                 mongodb: Union[MongoDB, dict] = None, db: str = None, collection: str = None, *args, **kwargs):
        super().__init__(item_type=item_type)
        self.mongodb = None
        self.db = None
        self.collection = None
        self.new_connection = None
        self.init(mongodb=mongodb, db=db, collection=collection, *args, **kwargs)

    def init(self, mongodb: Union[MongoDB, dict] = None, db: str = None, collection: str = None, *args, **kwargs):
        self.db = db
        self.collection = collection
        if isinstance(mongodb, MongoDB):
            self.mongodb = mongodb
            self.new_connection = False
        elif isinstance(mongodb, dict):
            self.mongodb = MongoDB(mongodb, *args, **kwargs)
            self.new_connection = True
        else:
            raise TypeError(type(mongodb))

    def close(self):
        if self.new_connection:
            self.mongodb.close()
        self.mongodb = None

    def exist(self, key: Union[str, list]):
        if isinstance(key, str):
            return self.mongodb[self.db][self.collection].count_documents({'_id': key}) > 0
        elif isinstance(key, list):
            return self.mongodb[self.db][self.collection].count_documents({'_id': {'$in': key}}) == len(key)
        else:
            raise TypeError(key)

    def remove(self, key: Union[str, list]):
        if isinstance(key, str):
            self.mongodb[self.db][self.collection].delete_one({'_id': key})
        elif isinstance(key, list):
            self.mongodb[self.db][self.collection].delete_many({'_id': {'$in': key}})
        else:
            raise TypeError(key)
