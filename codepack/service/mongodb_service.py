from codepack.interface import MongoDB
from codepack.utils.config import get_config, get_config_assertion_error_message
import os


class MongoDBService:
    def __init__(self, mongodb=None, config_path=None, section=None, conn_config=None, ssh_config=None,
                 db=None, collection=None, **kwargs):
        self.mongodb = None
        self.db = None
        self.collection = None
        self.new_connection = None
        self.link_to_mongodb(mongodb=mongodb, config_path=config_path,
                             section=section,
                             conn_config=conn_config, ssh_config=ssh_config,
                             db=db, collection=collection, **kwargs)

    def link_to_mongodb(self, mongodb=None, config_path=None, section=None, conn_config=None, ssh_config=None,
                        db=None, collection=None, **kwargs):
        mongodb, db, collection, new_connection = self.get_mongodb(mongodb=mongodb, config_path=config_path,
                                                                   section=section,
                                                                   conn_config=conn_config, ssh_config=ssh_config,
                                                                   db=db, collection=collection, **kwargs)
        self.mongodb = mongodb
        self.db = db
        self.collection = collection
        self.new_connection = new_connection

    @classmethod
    def get_db_info(cls, section=None, config_path=None, conn_config=None, db=None, collection=None):
        if not config_path:
            config_path = os.environ.get('CODEPACK_CONFIG_PATH', None)
        if config_path:
            if not conn_config or not db or not collection:
                config = get_config(config_path, section=section)
                if not conn_config:
                    if config['source'] not in ['mongodb']:
                        raise NotImplementedError("'%s' is unknown data source")
                    conn_config_path = get_config(config_path, section='conn')['path']
                    conn_config = get_config(conn_config_path, section=config['source'])
                if not db:
                    db = config['db']
                if not collection:
                    collection = config['collection']
        else:
            for k, v in {'db': db, 'collection': collection}.items():
                if not v:
                    raise AssertionError(get_config_assertion_error_message(k))
        return conn_config, db, collection

    @classmethod
    def get_mongodb(cls, mongodb=None, config_path=None, section=None, conn_config=None, ssh_config=None,
                    db=None, collection=None, **kwargs):
        new_connection = False
        if not mongodb:
            conn_config, db, collection = cls.get_db_info(config_path=config_path, section=section,
                                                          conn_config=conn_config, db=db, collection=collection)
            if conn_config:
                mongodb = MongoDB(config=conn_config, ssh_config=ssh_config, **kwargs)
                new_connection = True
        else:
            _, db, collection = cls.get_db_info(config_path=config_path, section=section,
                                                conn_config=mongodb.config, db=db, collection=collection)
        return mongodb, db, collection, new_connection
