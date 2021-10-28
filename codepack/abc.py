import abc
import bson
import json
import dill
import os
from codepack.interface import MongoDB
from codepack.utils import get_config


class Storable(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self):
        """initialize an instance"""
        self.id = None

    def to_json(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, j):
        d = json.loads(j)
        return cls.from_dict(d)

    def to_binary(self):
        return bson.Binary(dill.dumps(self))

    @classmethod
    def from_binary(cls, b):
        return dill.loads(b)

    def to_file(self, filename):
        with open(filename, 'w') as f:
            f.write(self.to_json())

    @classmethod
    def from_file(cls, filename):
        with open(filename, 'r') as f:
            ret = cls.from_json(f.read())
        return ret

    def to_db(self, db=None, collection=None, config=None, ssh_config=None, mongodb=None, **kwargs):
        # tmp = self.clone()
        mongodb, db, collection = MongoDBService.get_mongodb(section=self.__class__.__name__.lower(),
                                                             conn_config=config,
                                                             ssh_config=ssh_config,
                                                             db=db, collection=collection,
                                                             mongodb=mongodb, **kwargs)
        d = self.to_dict()
        mongodb[db][collection].insert_one(d)
        mongodb.close()

    @classmethod
    def from_db(cls, id, db=None, collection=None, config=None, ssh_config=None, mongodb=None, **kwargs):
        mongodb, db, collection = MongoDBService.get_mongodb(section=cls.__name__.lower(),
                                                             conn_config=config,
                                                             ssh_config=ssh_config,
                                                             db=db, collection=collection,
                                                             mongodb=mongodb, **kwargs)
        d = mongodb[db][collection].find_one({'_id': id})
        mongodb.close()
        if d is None:
            return d
        else:
            return cls.from_dict(d)

    @classmethod
    def get_db_info(cls, section=None, config_filepath=None, conn_config=None, db=None, collection=None):
        if not config_filepath:
            config_filepath = os.environ.get('CODEPACK_CONFIG_FILEPATH', None)
        if config_filepath:
            config = get_config(config_filepath, section=section)
            if not conn_config:
                if config['source'] not in ['mongodb']:
                    raise NotImplementedError("'%s' is unknown data source")
                conn_config_filepath = get_config(config_filepath, section='conn')['filepath']
                conn_config = get_config(conn_config_filepath, section=config['source'])
            if not db:
                db = config['db']
            if not collection:
                collection = config['collection']
        return conn_config, db, collection

    @abc.abstractmethod
    def to_dict(self):
        pass

    @classmethod
    @abc.abstractmethod
    def from_dict(cls, d):
        pass


class CodeBase(Storable, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self):
        super().__init__()


class CodePackBase(Storable, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self):
        super().__init__()


class MongoDBService:
    def __init__(self, db=None, collection=None, config=None, ssh_config=None, mongodb=None, offline=False, **kwargs):
        if not offline:
            mongodb, db, collection = self.get_mongodb(db=db, collection=collection,
                                                       config=config, ssh_config=ssh_config, mongodb=mongodb, **kwargs)
        self.mongodb = mongodb
        self.db = db
        self.collection = collection

    @classmethod
    def get_mongodb(cls, section=None, config_filepath=None, conn_config=None, ssh_config=None,
                    db=None, collection=None, mongodb=None, **kwargs):
        conn_config, db, collection = Storable.get_db_info(section=section, config_filepath=config_filepath,
                                                           conn_config=conn_config, db=db, collection=collection)
        if conn_config and not mongodb:
            mongodb = MongoDB(config=conn_config, ssh_config=ssh_config, **kwargs)
        return mongodb, db, collection
