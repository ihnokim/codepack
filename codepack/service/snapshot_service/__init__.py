from codepack.service.snapshot_service.snapshot_service import SnapshotService
from codepack.service.snapshot_service.memory_snapshot_service import MemorySnapshotService
from codepack.service.snapshot_service.file_snapshot_service import FileSnapshotService
from codepack.service.snapshot_service.mongo_snapshot_service import MongoSnapshotService
from enum import Enum


class SnapshotServiceAlias(Enum):
    MEMORY = MemorySnapshotService
    FILE = FileSnapshotService
    MONGODB = MongoSnapshotService
