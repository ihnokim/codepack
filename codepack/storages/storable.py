import abc
import bson
import json
import dill
from datetime import datetime
from bson import json_util
import os
from posixpath import join as posixpath_join
from typing import Any, Optional, Union, TypeVar


MongoDB = TypeVar('MongoDB', bound='codepack.interfaces.mongodb.MongoDB')  # noqa: F821
MongoClient = TypeVar('MongoClient', bound='pymongo.mongo_client.MongoClient')  # noqa: F821


class Storable(metaclass=abc.ABCMeta):
    def __init__(self, id: Optional[str] = None,
                 serial_number: Optional[str] = None,
                 version: Optional[str] = None,
                 timestamp: Optional[float] = None,
                 *args: Any, **kwargs: Any) -> None:
        self._id = id
        _, self._version = self.parse_id_and_version(key=id)
        self.serial_number = serial_number if serial_number else self.generate_serial_number()
        self._timestamp = None
        self.set_timestamp(timestamp=timestamp)
        if version is not None:
            self.set_version(version=version)

    @classmethod
    def parse_id_and_version(cls, key: Optional[str] = None) -> tuple:
        if key is None:
            return None, None
        try:
            idx = key.rindex('@')
            return key[:idx], key[idx+1:]
        except ValueError:
            return key, None

    def get_id(self) -> str:
        return self._id

    def set_id(self, id: Optional[str] = None) -> None:
        self._id = id

    def get_version(self) -> Optional[str]:
        return self._version

    def set_version(self, version: str) -> None:
        _id, _version = self.parse_id_and_version(self._id)
        self._version = version
        if _id is not None:
            self._id = '%s@%s' % (_id, self._version)

    def get_timestamp(self) -> float:
        return self._timestamp

    def set_timestamp(self, timestamp: float):
        self._timestamp = timestamp if timestamp else datetime.now().timestamp()

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

    def get_meta(self) -> dict:
        return {'_id': self.get_id(), 'serial_number': self.serial_number, '_timestamp': self.get_timestamp()}

    def diff(self, other: Union['Storable', dict]) -> dict:
        ret = dict()
        if isinstance(other, self.__class__):
            return self.diff(other.to_dict())
        elif isinstance(other, dict):
            tmp = self.to_dict()
            for k, v in other.items():
                if k not in tmp.keys():
                    ret[k] = v
                elif v != tmp[k]:
                    ret[k] = v
        else:
            raise TypeError(type(other))  # pragma: no cover
        return ret
