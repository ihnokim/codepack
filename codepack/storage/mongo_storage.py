from codepack.interface import MongoDB
from codepack.storage import Storage, Storable
from typing import Union
from typing import Type


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
