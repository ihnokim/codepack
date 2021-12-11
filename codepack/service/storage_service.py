from codepack.service.abc import StorageService
from codepack.service.mongodb_service import MongoDBService
from collections.abc import Iterable
from codepack.utils import Singleton
import os


class MemoryStorageService(StorageService, Singleton):
    def __init__(self, obj=None):
        super().__init__()
        if not hasattr(self, 'storage'):
            self.storage = dict()
        self.obj = obj

    def init(self):
        self.storage = dict()

    def save(self, obj):
        self.storage[obj.id] = obj

    def load(self, id):
        return self.storage[id]

    def remove(self, id):
        self.storage.pop(id, None)

    def check(self, id):
        if isinstance(id, str):
            if id in self.storage:
                return {self.storage[id].id: True}
            else:
                return None
        elif isinstance(id, Iterable):
            ret = list()
            for i in id:
                tmp = self.check(i)
                if tmp:
                    ret.append(tmp)
            return ret
        else:
            raise TypeError(type(id))  # pragma: no cover


class FileStorageService(StorageService):
    def __init__(self, obj, path='./'):
        super().__init__()
        self.obj = obj
        self.path = path

    @staticmethod
    def get_path(id, path='./'):
        return '%s%s.json' % (path, id)

    def save(self, obj, path=None):
        obj.to_file(self.get_path(id=obj.id, path=path if path else self.path))

    def load(self, id, obj=None, path=None):
        if obj is None:
            obj = self.obj
        return obj.from_file(path=self.get_path(id=id, path=path if path else self.path))

    def check(self, id, obj=None, path=None):
        if isinstance(id, str):
            if obj is None:
                obj = self.obj
            d = None
            try:
                tmp = obj.from_file(path=self.get_path(id=id, path=path if path else self.path))
                d = {tmp.id: True}
            finally:
                return d
        elif isinstance(id, Iterable):
            ret = list()
            for i in id:
                tmp = self.check(i)
                if tmp:
                    ret.append(tmp)
            return ret
        else:
            raise TypeError(type(id))  # pragma: no cover

    def remove(self, id, path=None):
        os.remove(path=self.get_path(id=id, path=path if path else self.path))


class MongoStorageService(StorageService, MongoDBService):
    def __init__(self, obj, db=None, collection=None,
                 mongodb=None, *args, **kwargs):
        MongoDBService.__init__(self, db=db, collection=collection,
                                mongodb=mongodb, *args, **kwargs)
        StorageService.__init__(self)
        self.obj = obj

    def save(self, obj, db=None, collection=None):
        obj.to_db(mongodb=self.mongodb,
                  db=db if db else self.db, collection=collection if collection else self.collection)

    def load(self, id, obj=None, db=None, collection=None):
        if obj is None:
            self.obj = obj
        return obj.from_db(id=id, mongodb=self.mongodb,
                           db=db if db else self.db, collection=collection if collection else self.collection)

    def check(self, id, obj=None, db=None, collection=None):
        if isinstance(id, str):
            if obj is None:
                obj = self.obj
            d = None
            try:
                tmp = obj.from_db(id=id, mongodb=self.mongodb,
                                  db=db if db else self.db, collection=collection if collection else self.collection)
                d = {tmp.id: True}
            finally:
                return d
        elif isinstance(id, Iterable):
            ret = list()
            for i in id:
                tmp = self.check(i)
                if tmp:
                    ret.append(tmp)
            return ret
        else:
            raise TypeError(type(id))  # pragma: no cover

    def remove(self, id, db=None, collection=None):
        self.mongodb[db if db else self.db][collection if collection else self.collection]\
            .delete_one({'_id': id})
