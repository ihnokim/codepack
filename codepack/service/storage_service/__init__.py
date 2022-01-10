from codepack.service.storage_service.storage_service import StorageService
from codepack.service.storage_service.memory_storage_service import MemoryStorageService
from codepack.service.storage_service.file_storage_service import FileStorageService
from codepack.service.storage_service.mongo_storage_service import MongoStorageService
from enum import Enum


class StorageServiceAlias(Enum):
    MEMORY = MemoryStorageService
    FILE = FileStorageService
    MONGODB = MongoStorageService
