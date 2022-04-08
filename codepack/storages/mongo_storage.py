from codepack.interfaces.mongodb import MongoDB
from codepack.storages.storage import Storage
from codepack.storages.storable import Storable
from typing import Type, Optional, Union, Any


class MongoStorage(Storage):
    def __init__(self, item_type: Optional[Type[Storable]] = None, key: str = 'serial_number',
                 mongodb: Optional[Union[MongoDB, dict]] = None,
                 db: Optional[str] = None, collection: Optional[str] = None, *args: Any, **kwargs: Any) -> None:
        super().__init__(item_type=item_type, key=key)
        self.mongodb = None
        self.db = None
        self.collection = None
        self.new_connection = None
        self.init(mongodb=mongodb, db=db, collection=collection, *args, **kwargs)

    def init(self, mongodb: Optional[Union[MongoDB, dict]] = None,
             db: Optional[str] = None, collection: Optional[str] = None, *args: Any, **kwargs: Any) -> None:
        self.db = db
        self.collection = collection
        if isinstance(mongodb, MongoDB):
            self.mongodb = mongodb
            self.new_connection = False
        elif isinstance(mongodb, dict):
            self.mongodb = MongoDB(mongodb, *args, **kwargs)
            self.new_connection = True
        else:
            raise TypeError(type(mongodb))  # pragma: no cover

    def close(self) -> None:
        if self.new_connection:
            self.mongodb.close()
        self.mongodb = None

    def exist(self, key: Union[str, list], summary: str = '') -> Union[bool, list]:
        if isinstance(key, str):
            return self.mongodb[self.db][self.collection].count_documents({'_id': key}) > 0
        elif isinstance(key, list):
            _summary, _ = self._validate_summary(summary=summary)
            if _summary == 'and':
                return self.mongodb[self.db][self.collection].count_documents({'_id': {'$in': key}}) == len(key)
            elif _summary == 'or':
                return self.mongodb[self.db][self.collection].count_documents({'_id': {'$in': key}}) > 0
            elif _summary == '':
                tmp = self.mongodb[self.db][self.collection].find({'_id': {'$in': key}}, projection={'_id': True})
                existing_keys = {k['_id'] for k in tmp}
                return [True if k in existing_keys else False for k in key]
        else:
            raise TypeError(key)  # pragma: no cover

    def remove(self, key: Union[str, list]) -> None:
        if isinstance(key, str):
            self.mongodb[self.db][self.collection].delete_one({'_id': key})
        elif isinstance(key, list):
            self.mongodb[self.db][self.collection].delete_many({'_id': {'$in': key}})
        else:
            raise TypeError(key)  # pragma: no cover

    def search(self, key: str, value: Any, projection: Optional[list] = None, to_dict: bool = False) -> list:
        if projection:
            to_dict = True
            _projection = {k: True for k in projection}
            _projection[self.key] = True
            if '_id' not in projection and self.key != '_id':
                _projection['_id'] = False
        else:
            _projection = projection
        search_result = self.mongodb[self.db][self.collection].find({key: value}, projection=_projection)
        if not search_result:
            return list()
        if to_dict:
            return list(search_result)
        else:
            return [self.item_type.from_dict(d) for d in search_result]

    def list_all(self) -> list:
        search_result = self.mongodb[self.db][self.collection].find(projection={'_id': 1})
        return [x['_id'] for x in search_result]

    def save(self, item: Union[Storable, list], update: bool = False) -> None:
        if isinstance(item, self.item_type):
            item_key = getattr(item, self.key)
            if not update and self.exist(key=item_key):
                raise ValueError('%s already exists' % item_key)
            else:
                d = item.to_dict()
                d.pop('_id', None)
                self.mongodb[self.db][self.collection].update_one({'_id': item_key}, {'$set': d}, upsert=True)
        elif isinstance(item, list):
            for i in item:
                self.save(item=i, update=update)
        else:
            raise TypeError(item)  # pragma: no cover

    def update(self, key: Union[str, list], values: dict) -> None:
        if len(values) > 0:
            if isinstance(key, str):
                self.mongodb[self.db][self.collection].update_one({'_id': key}, {'$set': values})
            elif isinstance(key, list):
                self.mongodb[self.db][self.collection].update_many({'_id': {'$in': key}}, {'$set': values})
            else:
                raise TypeError(type(key))  # pragma: no cover

    def load(self, key: Union[str, list], projection: Optional[list] = None, to_dict: bool = False)\
            -> Optional[Union[Storable, dict, list]]:
        if projection:
            to_dict = True
            _projection = {k: True for k in projection}
            _projection[self.key] = True
            if '_id' not in projection and self.key != '_id':
                _projection['_id'] = False
        else:
            _projection = projection
        if isinstance(key, str):
            d = self.mongodb[self.db][self.collection].find_one({'_id': key}, projection=_projection)
            if not d:
                return None
            elif to_dict:
                return d
            else:
                return self.item_type.from_dict(d)
        elif isinstance(key, list):
            tmp = self.mongodb[self.db][self.collection].find({'_id': {'$in': key}}, projection=_projection)
            if not tmp:
                return None
            elif to_dict:
                return list(tmp)
            else:
                return [self.item_type.from_dict(d) for d in tmp]
        else:
            raise TypeError(type(key))  # pragma: no cover
