from codepack import Code
from tests import *
from codepack.utils.config import get_default_service_config
import pytest
import os
from codepack.service import MemoryDeliveryService, FileSnapshotService, MongoStorageService


def test_no_config():
    with pytest.raises(AssertionError):
        Code(add2)


def test_default_config_with_os_env(default_os_env):
    code = Code(add2)
    assert code(1, 2) == 3


def test_config_path():
    code = Code(add2, config_path='config/test.ini')
    assert code(1, 2) == 3


def test_default_memory_code_snapshot_service_with_os_env(default_services):
    with pytest.raises(AssertionError):
        get_default_service_config('code_snapshot')
    env_source = 'CODEPACK_CODE_SNAPSHOT_SOURCE'
    try:
        os.environ[env_source] = 'MEMORY'
        mss = default_services.get_default_code_snapshot_service()
        assert hasattr(mss, 'memory')
    finally:
        if env_source in os.environ:
            os.environ.pop(env_source, None)


def test_default_file_code_snapshot_service_with_os_env(default_services):
    with pytest.raises(AssertionError):
        get_default_service_config('code_snapshot')
    env_source = 'CODEPACK_CODE_SNAPSHOT_SOURCE'
    env_path = 'CODEPACK_CODE_SNAPSHOT_PATH'
    try:
        os.environ[env_source] = 'FILE'
        os.environ[env_path] = 'tmp/'
        fss = default_services.get_default_code_snapshot_service()
        assert hasattr(fss, 'path')
        assert fss.path == 'tmp/'
    finally:
        for env in [env_source, env_path]:
            if env in os.environ:
                os.environ.pop(env, None)


def test_default_mongo_code_snapshot_service_with_os_env(default_services):
    with pytest.raises(AssertionError):
        get_default_service_config('code_snapshot')
    env_source = 'CODEPACK_CODE_SNAPSHOT_SOURCE'
    env_db = 'CODEPACK_CODE_SNAPSHOT_DB'
    env_collection = 'CODEPACK_CODE_SNAPSHOT_COLLECTION'
    env_config_path = 'CODEPACK_CONN_PATH'
    mss = None
    try:
        os.environ[env_source] = 'MONGODB'
        os.environ[env_db] = 'test'
        os.environ[env_collection] = 'snapshot'
        os.environ[env_config_path] = 'config/test_conn.ini'
        mss = default_services.get_default_code_snapshot_service()
        assert hasattr(mss, 'mongodb')
    finally:
        for env in [env_source, env_db, env_collection, env_config_path]:
            if env in os.environ:
                os.environ.pop(env, None)
        if mss is not None and not mss.mongodb.closed:
            mss.mongodb.close()


def test_default_memory_delivery_service_with_os_env(default_services):
    with pytest.raises(AssertionError):
        get_default_service_config('cache')
    env_source = 'CODEPACK_CACHE_SOURCE'
    try:
        os.environ[env_source] = 'MEMORY'
        mds = default_services.get_default_delivery_service()
        assert hasattr(mds, 'memory')
    finally:
        if env_source in os.environ:
            os.environ.pop(env_source, None)


def test_default_file_delivery_service_with_os_env(default_services):
    with pytest.raises(AssertionError):
        get_default_service_config('cache')
    env_source = 'CODEPACK_CACHE_SOURCE'
    env_path = 'CODEPACK_CACHE_PATH'
    try:
        os.environ[env_source] = 'FILE'
        os.environ[env_path] = 'tmp/'
        fds = default_services.get_default_delivery_service()
        assert hasattr(fds, 'path')
        assert fds.path == 'tmp/'
    finally:
        for env in [env_source, env_path]:
            if env in os.environ:
                os.environ.pop(env, None)


def test_default_mongo_delivery_service_with_os_env(default_services):
    with pytest.raises(AssertionError):
        get_default_service_config('cache')
    env_source = 'CODEPACK_CACHE_SOURCE'
    env_db = 'CODEPACK_CACHE_DB'
    env_collection = 'CODEPACK_CACHE_COLLECTION'
    env_config_path = 'CODEPACK_CONN_PATH'
    mds = None
    try:
        os.environ[env_source] = 'MONGODB'
        os.environ[env_db] = 'test'
        os.environ[env_collection] = 'cache'
        os.environ[env_config_path] = 'config/test_conn.ini'
        mds = default_services.get_default_delivery_service()
        assert hasattr(mds, 'mongodb')
    finally:
        for env in [env_source, env_db, env_collection, env_config_path]:
            if env in os.environ:
                os.environ.pop(env, None)
        if mds is not None and not mds.mongodb.closed:
            mds.mongodb.close()


def test_default_memory_code_storage_service_with_os_env(default_services):
    with pytest.raises(AssertionError):
        get_default_service_config('code')
    env_source = 'CODEPACK_CODE_STORAGE_SOURCE'
    try:
        os.environ[env_source] = 'MEMORY'
        mss = default_services.get_default_code_storage_service(obj=Code)
        assert hasattr(mss, 'memory')
        assert mss.obj == Code
    finally:
        if env_source in os.environ:
            os.environ.pop(env_source, None)


def test_default_file_code_storage_service_with_os_env(default_services):
    with pytest.raises(AssertionError):
        get_default_service_config('cache')
    env_source = 'CODEPACK_CODE_STORAGE_SOURCE'
    env_path = 'CODEPACK_CODE_STORAGE_PATH'
    try:
        os.environ[env_source] = 'FILE'
        os.environ[env_path] = 'tmp/'
        fss = default_services.get_default_code_storage_service(obj=Code)
        assert hasattr(fss, 'path')
        assert fss.path == 'tmp/'
        assert fss.obj == Code
    finally:
        for env in [env_source, env_path]:
            if env in os.environ:
                os.environ.pop(env, None)


def test_default_mongo_code_storage_service_with_os_env(default_services):
    with pytest.raises(AssertionError):
        get_default_service_config('cache')
    env_source = 'CODEPACK_CODE_STORAGE_SOURCE'
    env_db = 'CODEPACK_CODE_STORAGE_DB'
    env_collection = 'CODEPACK_CODE_STORAGE_COLLECTION'
    env_config_path = 'CODEPACK_CONN_PATH'
    mss = None
    try:
        os.environ[env_source] = 'MONGODB'
        os.environ[env_db] = 'test'
        os.environ[env_collection] = 'codes'
        os.environ[env_config_path] = 'config/test_conn.ini'
        mss = default_services.get_default_code_storage_service(obj=Code)
        assert hasattr(mss, 'mongodb')
        assert mss.obj == Code
    finally:
        for env in [env_source, env_db, env_collection, env_config_path]:
            if env in os.environ:
                os.environ.pop(env, None)
        if mss is not None and not mss.mongodb.closed:
            mss.mongodb.close()


def test_if_default_services_have_single_instance_for_each_service(default_services, testdir_snapshot_service):
    os.environ['CODEPACK_CACHE_SOURCE'] = 'MEMORY'
    os.environ['CODEPACK_CODE_SNAPSHOT_SOURCE'] = 'FILE'
    os.environ['CODEPACK_CODE_SNAPSHOT_PATH'] = testdir_snapshot_service
    os.environ['CODEPACK_CODE_STORAGE_SOURCE'] = 'MONGODB'
    os.environ['CODEPACK_CODE_STORAGE_DB'] = 'test'
    os.environ['CODEPACK_CODE_STORAGE_COLLECTION'] = 'codes'
    os.environ['CODEPACK_CONN_PATH'] = 'config/test_conn.ini'
    try:
        code1 = Code(add2)
        code2 = Code(add3)
        assert isinstance(code1.service['delivery_service'], MemoryDeliveryService)
        assert isinstance(code1.service['snapshot_service'], FileSnapshotService)
        assert isinstance(code1.service['storage_service'], MongoStorageService)
        assert isinstance(code2.service['delivery_service'], MemoryDeliveryService)
        assert isinstance(code2.service['snapshot_service'], FileSnapshotService)
        assert isinstance(code2.service['storage_service'], MongoStorageService)

        assert (code1.service['delivery_service']) == (code2.service['delivery_service'])
        assert (code1.service['snapshot_service']) == (code2.service['snapshot_service'])
        assert (code1.service['storage_service']) == (code2.service['storage_service'])

        assert id(code1.service['delivery_service']) == id(code2.service['delivery_service'])
        assert id(code1.service['snapshot_service']) == id(code2.service['snapshot_service'])
        assert id(code1.service['storage_service']) == id(code2.service['storage_service'])
    finally:
        for env in ['CODEPACK_CACHE_SOURCE',
                    'CODEPACK_CODE_SNAPSHOT_SOURCE', 'CODEPACK_CODE_SNAPSHOT_PATH',
                    'CODEPACK_CODE_STORAGE_SOURCE', 'CODEPACK_CODE_STORAGE_DB', 'CODEPACK_CODE_STORAGE_COLLECTION',
                    'CODEPACK_CONN_PATH']:
            os.environ.pop(env, None)
