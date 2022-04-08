from codepack import Alias, StorageService
from codepack.storages import MemoryStorage
import os
import pytest
import configparser


def test_alias_from_individual_os_env():
    a = Alias()
    assert a.aliases is None
    assert isinstance(a['storage_service'], StorageService.__class__)
    os_env = 'CODEPACK_ALIAS_STORAGE_SERVICE'
    try:
        os.environ[os_env] = 'codepack.storages.memory_storage.MemoryStorage'
        assert a['storage_service'] == MemoryStorage
    finally:
        os.environ.pop(os_env, None)


def test_alias_from_alias_path_os_env():
    a = Alias()
    assert a.aliases is None
    assert isinstance(a['storage_service'], StorageService.__class__)
    os_env = 'CODEPACK_ALIAS_PATH'
    try:
        os.environ[os_env] = 'codepack/utils/config/default/alias.ini'
        assert a['storage_service'] == StorageService
    finally:
        os.environ.pop(os_env, None)


def test_alias_from_config_path_os_env():
    a = Alias()
    assert a.aliases is None
    assert isinstance(a['storage_service'], StorageService.__class__)
    os_env1 = 'CODEPACK_CONFIG_DIR'
    os_env2 = 'CODEPACK_CONFIG_PATH'
    try:
        os.environ[os_env1] = 'config'
        os.environ[os_env2] = 'test.ini'
        assert a['storage_service'] == StorageService
    finally:
        os.environ.pop(os_env1, None)
        os.environ.pop(os_env2, None)


def test_alias_priority1():
    a = Alias(data={'storage_service': 'codepack.storages.memory_storage.MemoryStorage'})
    assert a.aliases is not None
    assert a['storage_service'] == MemoryStorage
    os_env1 = 'CODEPACK_ALIAS_STORAGE_SERVICE'
    os_env2 = 'CODEPACK_ALIAS_PATH'
    os_env3 = 'CODEPACK_CONFIG_PATH'
    try:
        os.environ[os_env1] = 'codepack.plugins.snapshot_service.SnapshotService'
        assert a['storage_service'] == MemoryStorage
        os.environ.pop(os_env1, None)
        os.environ[os_env2] = 'codepack/config/default/alias.ini'
        assert a['storage_service'] == MemoryStorage
        os.environ.pop(os_env2, None)
        os.environ[os_env3] = 'config/test.ini'
        assert a['storage_service'] == MemoryStorage
    finally:
        os.environ.pop(os_env1, None)
        os.environ.pop(os_env2, None)
        os.environ.pop(os_env3, None)


def test_alias_priority2():
    a = Alias()
    assert a.aliases is None
    assert isinstance(a['storage_service'], StorageService.__class__)
    first_os_env = 'CODEPACK_ALIAS_STORAGE_SERVICE'
    second_os_env = 'CODEPACK_ALIAS_PATH'
    third_os_env = 'CODEPACK_CONFIG_PATH'
    try:
        os.environ[first_os_env] = 'codepack.storages.memory_storage.MemoryStorage'
        assert a['storage_service'] == MemoryStorage
        os.environ[second_os_env] = 'codepack/config/default/alias.ini'
        assert a['storage_service'] == MemoryStorage
        os.environ.pop(second_os_env)
        os.environ[third_os_env] = 'config/test.ini'
        assert a['storage_service'] == MemoryStorage
    finally:
        os.environ.pop(first_os_env, None)
        os.environ.pop(second_os_env, None)
        os.environ.pop(third_os_env, None)


def test_alias_path_argument():
    os_env = 'CODEPACK_CONFIG_DIR'
    try:
        a = Alias(data='codepack/utils/config/default/alias.ini')
        assert a['storage_service'] == StorageService
        os.environ[os_env] = 'config'
        with pytest.raises(configparser.NoSectionError):
            Alias(data='config/test.ini')
    finally:
        os.environ.pop(os_env, None)


def test_default_alias():
    a = Alias()
    assert a is not None
    assert a.aliases is None
    default_aliases = a.get_default_alias()
    assert isinstance(default_aliases, dict) and len(default_aliases) > 0
    assert a['memory_storage'] == MemoryStorage
    assert a['storage_service'] == StorageService
