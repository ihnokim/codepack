from codepack.config import Alias
import os
import pytest
from codepack.service import StorageService


def test_alias_from_individual_os_env():
    a = Alias()
    assert a.aliases is None
    with pytest.raises(AttributeError):
        tmp = a['storage_service']
    os_env = 'CODEPACK_ALIAS_STORAGE_SERVICE'
    os.environ[os_env] = 'codepack.service.storage_service.StorageService'
    assert a['storage_service'] == StorageService
    os.environ.pop(os_env)


def test_alias_from_alias_path_os_env():
    a = Alias()
    assert a.aliases is None
    with pytest.raises(AttributeError):
        tmp = a['storage_service']
    os_env = 'CODEPACK_ALIAS_PATH'
    os.environ[os_env] = 'config/alias.ini'
    assert a['storage_service'] == StorageService
    os.environ.pop(os_env)


def test_alias_from_config_path_os_env():
    a = Alias()
    assert a.aliases is None
    with pytest.raises(AttributeError):
        tmp = a['storage_service']
    os_env1 = 'CODEPACK_CONFIG_DIR'
    os_env2 = 'CODEPACK_CONFIG_PATH'
    os.environ[os_env1] = 'config'
    os.environ[os_env2] = 'test.ini'
    assert a['storage_service'] == StorageService
    os.environ.pop(os_env2)


def test_alias_priority1():
    a = Alias(data={'storage_service': 'codepack.service.storage_service.StorageService'})
    assert a.aliases is not None
    assert a['storage_service'] == StorageService
    os_env = 'CODEPACK_ALIAS_STORAGE_SERVICE'
    os.environ[os_env] = 'codepack.service.snapshot_service.SnapshotService'
    assert a['storage_service'] == StorageService
    os.environ.pop(os_env)
    os_env = 'CODEPACK_ALIAS_PATH'
    os.environ[os_env] = 'config/alias.ini'
    assert a['storage_service'] == StorageService
    os.environ.pop(os_env)
    os_env = 'CODEPACK_CONFIG_PATH'
    os.environ[os_env] = 'config/test.ini'
    assert a['storage_service'] == StorageService
    os.environ.pop(os_env)


def test_alias_priority2():
    a = Alias()
    assert a.aliases is None
    with pytest.raises(AttributeError):
        tmp = a['storage_service']
    first_os_env = 'CODEPACK_ALIAS_STORAGE_SERVICE'
    os.environ[first_os_env] = 'codepack.service.storage_service.StorageService'
    assert a['storage_service'] == StorageService
    os_env = 'CODEPACK_ALIAS_PATH'
    os.environ[os_env] = 'config/alias.ini'
    assert a['storage_service'] == StorageService
    os.environ.pop(os_env)
    os_env = 'CODEPACK_CONFIG_PATH'
    os.environ[os_env] = 'config/test.ini'
    assert a['storage_service'] == StorageService
    os.environ.pop(os_env)
    os.environ.pop(first_os_env)


def test_alias_path_argument():
    a = Alias(data='config/alias.ini')
    assert a['storage_service'] == StorageService
    b = Alias(data='config/test.ini')
    assert b['storage_service'] == StorageService
