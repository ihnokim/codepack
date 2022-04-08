import abc
import bson
import json
import dill
from datetime import datetime
from bson import json_util
import os
from posixpath import join as posixpath_join
from typing import Any, Optional, Union, TypeVar


MongoDB = TypeVar('MongoDB', bound='codepack.interfaces.mongodb.MongoDB')
MongoClient = TypeVar('MongoClient', bound='pymongo.mongo_client.MongoClient')


class Storable(metaclass=abc.ABCMeta):
    def __init__(self, id: Optional[str] = None, serial_number: Optional[str] = None,
                 *args: Any, **kwargs: Any) -> None:
        """initialize instance"""
        self.id = id
        self.serial_number = serial_number if serial_number else self.generate_serial_number()

    def generate_serial_number(self) -> str:
        return (str(id(self)) + str(datetime.now().timestamp())).replace('.', '')

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=json_util.default)

    @classmethod
    def from_json(cls, j: Union[str, bytes]) -> 'Storable':
        d = json.loads(j, object_hook=json_util.object_hook)
        return cls.from_dict(d)

    def to_binary(self) -> bson.binary.Binary:
        return bson.Binary(dill.dumps(self))

    @classmethod
    def from_binary(cls, b: bson.binary.Binary) -> 'Storable':
        return dill.loads(b)

    def to_file(self, path: str) -> None:
        with open(path, 'w+') as f:
            f.write(self.to_json())

    @classmethod
    def from_file(cls, path: str) -> 'Storable':
        with open(path, 'r') as f:
            ret = cls.from_json(f.read())
        return ret

    def to_db(self, mongodb: Union[MongoDB, MongoClient], db: str, collection: str,
              *args: Any, **kwargs: Any) -> None:
        mongodb[db][collection].insert_one(self.to_dict(), *args, **kwargs)

    @classmethod
    def from_db(cls, id: id, mongodb: Union[MongoDB, MongoClient], db: str, collection: str,
                *args: Any, **kwargs: Any) -> Optional['Storable']:
        d = mongodb[db][collection].find_one({'_id': id}, *args, **kwargs)
        if d is None:
            return d
        else:
            return cls.from_dict(d)

    @abc.abstractmethod
    def to_dict(self) -> dict:
        """convert to dict"""

    @classmethod
    @abc.abstractmethod
    def from_dict(cls, d: dict) -> 'Storable':
        """create instance from dict"""

    @classmethod
    def get_path(cls, key: str, path: str = './', posix: bool = False) -> str:
        if posix:
            return posixpath_join(path, '%s.json' % key)
        else:
            return os.path.join(path, '%s.json' % key)
