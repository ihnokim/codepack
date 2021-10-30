import abc
import bson
import json
import dill
import os
from codepack.interface import MongoDB
from codepack.utils import get_config
from datetime import datetime


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
        mongodb, db, collection, new_connection = MongoDBService.get_mongodb(section=self.__class__.__name__.lower(),
                                                                             conn_config=config,
                                                                             ssh_config=ssh_config,
                                                                             db=db, collection=collection,
                                                                             mongodb=mongodb, **kwargs)
        d = self.to_dict()
        try:
            mongodb[db][collection].insert_one(d)
        finally:
            if new_connection:
                mongodb.close()

    @classmethod
    def from_db(cls, id, db=None, collection=None, config=None, ssh_config=None, mongodb=None, **kwargs):
        mongodb, db, collection, new_connection = MongoDBService.get_mongodb(section=cls.__name__.lower(),
                                                                             conn_config=config,
                                                                             ssh_config=ssh_config,
                                                                             db=db, collection=collection,
                                                                             mongodb=mongodb, **kwargs)
        try:
            d = mongodb[db][collection].find_one({'_id': id})
        finally:
            if new_connection:
                mongodb.close()
        if d is None:
            return d
        else:
            return cls.from_dict(d)

    @classmethod
    def _config_assertion_error_message(cls, x):
        return "'%s' should be given as an argument or defined in a configuration file in 'config_filepath' " \
               "or os.environ['CODEPACK_CONFIG_FILEPATH']" % x

    @classmethod
    def get_db_info(cls, section=None, config_filepath=None, conn_config=None, db=None, collection=None):
        if not config_filepath:
            config_filepath = os.environ.get('CODEPACK_CONFIG_FILEPATH', None)
        if config_filepath:
            if not conn_config or not db or not collection:
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
        else:
            for k, v in {'db': db, 'collection': collection}.items():
                if not v:
                    raise AssertionError(cls._config_assertion_error_message(k))
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
    def __init__(self, serial_number=None):
        super().__init__()
        self.serial_number = serial_number if serial_number else self.generate_serial_number()

    def generate_serial_number(self):
        return (str(id(self)) + str(datetime.now().timestamp())).replace('.', '')


class CodePackBase(Storable, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self):
        super().__init__()


class MongoDBService:
    def __init__(self, section=None, config_filepath=None, db=None, collection=None, conn_config=None, ssh_config=None,
                 mongodb=None, online=False, **kwargs):
        self.mongodb= None
        self.db = None
        self.collection = None
        self.new_connection = None
        self.online = False
        self.link_to_mongodb(section=section, config_filepath=config_filepath,
                             db=db, collection=collection,
                             conn_config=conn_config, ssh_config=ssh_config,
                             mongodb=mongodb, online=online, **kwargs)

    def link_to_mongodb(self, section=None, config_filepath=None, db=None, collection=None,
                        conn_config=None, ssh_config=None,
                        mongodb=None, online=False, **kwargs):
        new_connection = False
        self.online = online
        if self.online:
            mongodb, db, collection, new_connection = self.get_mongodb(section=section, config_filepath=config_filepath,
                                                                       db=db, collection=collection,
                                                                       conn_config=conn_config, ssh_config=ssh_config,
                                                                       mongodb=mongodb, **kwargs)
        self.mongodb = mongodb
        self.db = db
        self.collection = collection
        self.new_connection = new_connection

    @classmethod
    def get_mongodb(cls, section=None, config_filepath=None, conn_config=None, ssh_config=None,
                    db=None, collection=None, mongodb=None, **kwargs):
        new_connection = False
        if not mongodb:
            conn_config, db, collection = Storable.get_db_info(section=section, config_filepath=config_filepath,
                                                               conn_config=conn_config, db=db, collection=collection)
            if conn_config:
                mongodb = MongoDB(config=conn_config, ssh_config=ssh_config, **kwargs)
                new_connection = True
        else:
            _, db, collection = Storable.get_db_info(section=section, config_filepath=config_filepath,
                                                     conn_config=mongodb.config, db=db, collection=collection)
        return mongodb, db, collection, new_connection
