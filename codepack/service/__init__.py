from codepack.service.delivery_service import MemoryDeliveryService, FileDeliveryService, MongoDeliveryService
from codepack.service.state_manager import MemoryStateManager, FileStateManager, MongoStateManager
from codepack.service.storage_service import MemoryStorageService, FileStorageService, MongoStorageService
from codepack.service.mongodb_service import MongoDBService
from enum import Enum
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


def get_delivery_service(source, *args, **kwargs):
    cls = DeliveryServiceAlias[source].value
    return cls(*args, **kwargs)


def get_state_manager(source, *args, **kwargs):
    cls = StateManagerAlias[source].value
    return cls(*args, **kwargs)


def get_storage_service(source, *args, **kwargs):
    cls = StorageServiceAlias[source].value
    return cls(*args, **kwargs)


def get_default_delivery_service(config_path=None, *args, **kwargs):
    config = get_default_service_config(section='cache', config_path=config_path)
    return get_delivery_service(*args, **config, **kwargs)


def get_default_state_manager(config_path=None, *args, **kwargs):
    config = get_default_service_config(section='state', config_path=config_path)
    return get_state_manager(*args, **config, **kwargs)


def get_default_code_storage_service(obj, config_path=None, *args, **kwargs):
    config = get_default_service_config(section='code', config_path=config_path)
    return get_storage_service(obj=obj, *args, **config, **kwargs)


def get_default_codepack_storage_service(obj, config_path=None, *args, **kwargs):
    config = get_default_service_config(section='codepack', config_path=config_path)
    return get_storage_service(obj=obj, *args, **config, **kwargs)
