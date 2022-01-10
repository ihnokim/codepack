from codepack.service.delivery_service.delivery_service import DeliveryService
from codepack.service.delivery_service.memory_delivery_service import MemoryDeliveryService
from codepack.service.delivery_service.file_delivery_service import FileDeliveryService
from codepack.service.delivery_service.mongo_delivery_service import MongoDeliveryService

from enum import Enum


class DeliveryServiceAlias(Enum):
    MEMORY = MemoryDeliveryService
    FILE = FileDeliveryService
    MONGODB = MongoDeliveryService
