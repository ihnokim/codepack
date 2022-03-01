import abc
import bson
import json
import dill
from datetime import datetime
from bson import json_util
import os
from posixpath import join as posixpath_join


class Storable(metaclass=abc.ABCMeta):
    def __init__(self, id=None, serial_number=None, *args, **kwargs):
        """initialize instance"""
        self.id = id
        self.serial_number = serial_number if serial_number else self.generate_serial_number()

    def generate_serial_number(self):
        return (str(id(self)) + str(datetime.now().timestamp())).replace('.', '')

    def to_json(self):
        return json.dumps(self.to_dict(), default=json_util.default)

    @classmethod
    def from_json(cls, j):
        d = json.loads(j, object_hook=json_util.object_hook)
        return cls.from_dict(d)

    def to_binary(self):
        return bson.Binary(dill.dumps(self))

    @classmethod
    def from_binary(cls, b):
        return dill.loads(b)

    def to_file(self, path):
        with open(path, 'w+') as f:
            f.write(self.to_json())

    @classmethod
    def from_file(cls, path):
        with open(path, 'r') as f:
            ret = cls.from_json(f.read())
        return ret

    def to_db(self, mongodb, db, collection, *args, **kwargs):
        mongodb[db][collection].insert_one(self.to_dict(), *args, **kwargs)

    @classmethod
    def from_db(cls, id, mongodb, db, collection, *args, **kwargs):
        d = mongodb[db][collection].find_one({'_id': id}, *args, **kwargs)
        if d is None:
            return d
        else:
            return cls.from_dict(d)

    @abc.abstractmethod
    def to_dict(self):
        """convert to dict"""

    @classmethod
    @abc.abstractmethod
    def from_dict(cls, d):
        """create instance from dict"""

    @classmethod
    def get_path(cls, key, path='./', posix=False):
        if posix:
            return posixpath_join(path, '%s.json' % key)
        else:
            return os.path.join(path, '%s.json' % key)
