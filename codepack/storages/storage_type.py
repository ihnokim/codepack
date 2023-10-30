from codepack.storages.file_storage import FileStorage
from codepack.storages.memory_storage import MemoryStorage
from codepack.utils.type_manager import TypeManager


class StorageType(TypeManager):
    types = {'memory': MemoryStorage, 'file': FileStorage}
