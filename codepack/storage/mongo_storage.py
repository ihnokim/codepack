from codepack.interface import MongoDB
from codepack.storage import Storage


class MongoStorage(Storage):
    def __init__(self, obj=None, mongodb=None, db=None, collection=None, *args, **kwargs):
        super().__init__(obj=obj)
        self.mongodb = None
        self.db = None
        self.collection = None
        self.new_connection = None
        self.init(mongodb=mongodb, db=db, collection=collection, *args, **kwargs)

    def init(self, mongodb=None, db=None, collection=None, *args, **kwargs):
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
