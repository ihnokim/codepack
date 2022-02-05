from codepack.config import Alias
import os
import pytest
from codepack.service import MemoryStorageService, FileStorageService


def test_alias_from_individual_os_env():
    a = Alias()
    assert a.aliases is None
    with pytest.raises(AttributeError):
        tmp = a['memory_storage_service']
    os_env = 'CODEPACK_ALIAS_MEMORY_STORAGE_SERVICE'
    os.environ[os_env] = 'codepack.service.storage_service.memory_storage_service.MemoryStorageService'
    assert a['memory_storage_service'] == MemoryStorageService
    os.environ.pop(os_env)


def test_alias_from_alias_path_os_env():
    a = Alias()
    assert a.aliases is None
    with pytest.raises(AttributeError):
        tmp = a['memory_storage_service']
    os_env = 'CODEPACK_ALIAS_PATH'
    os.environ[os_env] = 'config/alias.ini'
    assert a['memory_storage_service'] == MemoryStorageService
    os.environ.pop(os_env)


def test_alias_from_config_path_os_env():
    a = Alias()
    assert a.aliases is None
    with pytest.raises(AttributeError):
        tmp = a['memory_storage_service']
    os_env = 'CODEPACK_CONFIG_PATH'
    os.environ[os_env] = 'config/test.ini'
    assert a['memory_storage_service'] == MemoryStorageService
    os.environ.pop(os_env)


def test_alias_priority1():
    a = Alias(data={'memory_storage_service': 'codepack.service.storage_service.file_storage_service.FileStorageService'})
    assert a.aliases is not None
    assert a['memory_storage_service'] == FileStorageService
    os_env = 'CODEPACK_ALIAS_MEMORY_STORAGE_SERVICE'
    os.environ[os_env] = 'codepack.service.storage_service.memory_storage_service.MemoryStorageService'
    assert a['memory_storage_service'] == FileStorageService
    os.environ.pop(os_env)
    os_env = 'CODEPACK_ALIAS_PATH'
    os.environ[os_env] = 'config/alias.ini'
    assert a['memory_storage_service'] == FileStorageService
    os.environ.pop(os_env)
    os_env = 'CODEPACK_CONFIG_PATH'
    os.environ[os_env] = 'config/test.ini'
    assert a['memory_storage_service'] == FileStorageService
    os.environ.pop(os_env)


def test_alias_priority2():
    a = Alias()
    assert a.aliases is None
    with pytest.raises(AttributeError):
        tmp = a['memory_storage_service']
    first_os_env = 'CODEPACK_ALIAS_MEMORY_STORAGE_SERVICE'
    os.environ[first_os_env] = 'codepack.service.storage_service.file_storage_service.FileStorageService'
    assert a['memory_storage_service'] == FileStorageService
    os_env = 'CODEPACK_ALIAS_PATH'
    os.environ[os_env] = 'config/alias.ini'
    assert a['memory_storage_service'] == FileStorageService
    os.environ.pop(os_env)
    os_env = 'CODEPACK_CONFIG_PATH'
    os.environ[os_env] = 'config/test.ini'
    assert a['memory_storage_service'] == FileStorageService
    os.environ.pop(os_env)
    os.environ.pop(first_os_env)


def test_alias_path_argument():
    a = Alias(data='config/alias.ini')
    assert a['memory_storage_service'] == MemoryStorageService
