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
    def __init__(self, name: Optional[str] = None,
                 serial_number: Optional[str] = None,
                 version: Optional[str] = None,
                 timestamp: Optional[float] = None,
                 *args: Any, **kwargs: Any) -> None:
        self._name = name
        _, self._version = self.parse_id_and_version(key=name)
        self._serial_number = None
        self.set_serial_number(serial_number=serial_number if serial_number else self.generate_serial_number())
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

    def get_name(self) -> str:
        return self._name

    def set_name(self, name: Optional[str] = None) -> None:
        self._name = name

    def get_version(self) -> Optional[str]:
        return self._version

    def set_version(self, version: str) -> None:
        _name, _version = self.parse_id_and_version(self._name)
        self._version = version
        if _name is not None:
            self._name = '%s@%s' % (_name, self._version)

    def get_serial_number(self) -> str:
        return self._serial_number

    def set_serial_number(self, serial_number: str) -> None:
        self._serial_number = serial_number

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
        return {'_name': self.get_name(), '_serial_number': self.get_serial_number(),
                '_timestamp': self.get_timestamp()}

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
