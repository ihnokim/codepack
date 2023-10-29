import abc
import json
from typing import Union, Optional, Dict, Any
from codepack.storages.storage import Storage
from codepack.utils.config import Config
from codepack.utils.factory import Factory
from codepack.storages.storage_type import StorageType
import uuid


class Item(metaclass=abc.ABCMeta):
    VERSION_DELIMITER: str = '@'
    storage: Storage = None

    def __init__(self,
                 name: Optional[str] = None,
                 version: Optional[str] = None,
                 owner: Optional[str] = None,
                 description: Optional[str] = None):
        self._id = str(uuid.uuid4())
        self.name = name
        self.version = version
        self.owner = owner
        self.description = description

    @abc.abstractmethod
    def __serialize__(self) -> Dict[str, Any]:
        pass  # pragma: no cover

    @classmethod
    @abc.abstractmethod
    def __deserialize__(cls, d: Dict[str, Any]) -> 'Item':
        pass  # pragma: no cover

    def get_id(self) -> str:
        return self._id

    def to_dict(self) -> Dict[str, Any]:
        d = self.__serialize__()
        d['_id'] = self.get_id()
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'Item':
        return cls.__deserialize__(d)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, j: Union[str, bytes]) -> 'Item':
        d = json.loads(j)
        return cls.from_dict(d)

    @classmethod
    def set_storage(cls, storage: Optional[Storage] = None) -> None:
        if not storage:
            config = Config()
            item_config = config.get_config(section=cls.__name__.lower())
            storage_type = item_config.get('type')
            storage_config_section = item_config.get('config')
            if storage_config_section:
                storage_config = config.get_config(section=storage_config_section)
            else:
                storage_config = None
            factory = Factory(type_manager=StorageType)
            cls.storage = factory.get_instance(type=storage_type,
                                               config=storage_config)
        else:
            cls.storage = storage

    @classmethod
    def get_storage(cls) -> Storage:
        if not cls.storage:
            cls.set_storage()
        return cls.storage

    def get_fullname(self) -> str:
        if self.version:
            return '%s%s%s' % (self.name, self.VERSION_DELIMITER, self.version)
        else:
            return self.name

    def save(self) -> bool:
        storage = self.__class__.get_storage()
        return storage.save(id=self.get_id(), item=self.to_dict())

    @classmethod
    def load(cls, id: str) -> 'Item':
        storage = cls.get_storage()
        d = storage.load(id=id)
        return cls.from_dict(d)
