from codepack.service.delivery_service import MemoryDeliveryService, FileDeliveryService, MongoDeliveryService
from codepack.service.state_manager import MemoryStateManager, FileStateManager, MongoStateManager
from codepack.service.storage_service import MemoryStorageService, FileStorageService, MongoStorageService
from codepack.service.mongodb_service import MongoDBService
from enum import Enum
from codepack.utils import Singleton
from codepack.utils.config import get_default_service_config


class DeliveryServiceAlias(Enum):
    MEMORY = MemoryDeliveryService
    FILE = FileDeliveryService
    MONGODB = MongoDeliveryService


class StateManagerAlias(Enum):
    MEMORY = MemoryStateManager
    FILE = FileStateManager
    MONGODB = MongoStateManager


class StorageServiceAlias(Enum):
    MEMORY = MemoryStorageService
    FILE = FileStorageService
    MONGODB = MongoStorageService


class DefaultServicePack(Singleton):
    delivery_service = None
    state_manager = None
    code_storage_service = None
    codepack_storage_service = None

    @classmethod
    def init(cls):
        cls.delivery_service = None
        cls.state_manager = None
        cls.code_storage_service = None
        cls.codepack_storage_service = None

    @classmethod
    def get_default_delivery_service(cls, config_path=None, *args, **kwargs):
        if not cls.delivery_service:
            config = get_default_service_config(section='cache', config_path=config_path)
            cls.delivery_service = get_delivery_service(*args, **config, **kwargs)
        return cls.delivery_service

    @classmethod
    def get_default_state_manager(cls, config_path=None, *args, **kwargs):
        if not cls.state_manager:
            config = get_default_service_config(section='state', config_path=config_path)
            cls.state_manager = get_state_manager(*args, **config, **kwargs)
        return cls.state_manager

    @classmethod
    def get_default_code_storage_service(cls, obj, config_path=None, *args, **kwargs):
        if not cls.code_storage_service:
            config = get_default_service_config(section='code', config_path=config_path)
            cls.code_storage_service = get_storage_service(obj=obj, *args, **config, **kwargs)
        return cls.code_storage_service

    @classmethod
    def get_default_codepack_storage_service(cls, obj, config_path=None, *args, **kwargs):
        if not cls.codepack_storage_service:
            config = get_default_service_config(section='codepack', config_path=config_path)
            cls.code_storage_service = get_storage_service(obj=obj, *args, **config, **kwargs)
        return cls.code_storage_service


def get_delivery_service(source, *args, **kwargs):
    cls = DeliveryServiceAlias[source].value
    return cls(*args, **kwargs)


def get_state_manager(source, *args, **kwargs):
    cls = StateManagerAlias[source].value
    return cls(*args, **kwargs)


def get_storage_service(source, *args, **kwargs):
    cls = StorageServiceAlias[source].value
    return cls(*args, **kwargs)
