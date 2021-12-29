from codepack.interface import MongoDB


class MongoDBService:
    def __init__(self, mongodb=None, db=None, collection=None, *args, **kwargs):
        self.mongodb = None
        self.db = None
        self.collection = None
        self.new_connection = None
        self.connect(mongodb=mongodb, db=db, collection=collection, *args, **kwargs)

    def connect(self, mongodb, db, collection, *args, **kwargs):
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
