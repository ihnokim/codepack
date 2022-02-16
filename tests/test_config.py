from codepack import Code
from tests import *
from codepack.config import Config
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


def test_default_memory_code_snapshot_service_with_os_env(default):
    config = Config()
    with pytest.raises(AssertionError):
        config.get_storage_config('code_snapshot')
    env_source = 'CODEPACK_CODE_SNAPSHOT_SOURCE'
    try:
        os.environ[env_source] = 'memory'
        mss = default.get_storage_instance('code_snapshot', 'snapshot_service')
        assert hasattr(mss, 'memory')
    finally:
        if env_source in os.environ:
            os.environ.pop(env_source, None)


def test_default_file_code_snapshot_service_with_os_env(default):
    config = Config()
    with pytest.raises(AssertionError):
        config.get_storage_config('code_snapshot')
    env_source = 'CODEPACK_CODE_SNAPSHOT_SOURCE'
    env_path = 'CODEPACK_CODE_SNAPSHOT_PATH'
    try:
        os.environ[env_source] = 'file'
        os.environ[env_path] = 'tmp/'
        fss = default.get_storage_instance('code_snapshot', 'snapshot_service')
        assert hasattr(fss, 'path')
        assert fss.path == 'tmp/'
    finally:
        for env in [env_source, env_path]:
            if env in os.environ:
                os.environ.pop(env, None)


def test_default_mongo_code_snapshot_service_with_os_env(default):
    config = Config()
    with pytest.raises(AssertionError):
        config.get_storage_config('code_snapshot')
    env_source = 'CODEPACK_CODE_SNAPSHOT_SOURCE'
    env_db = 'CODEPACK_CODE_SNAPSHOT_DB'
    env_collection = 'CODEPACK_CODE_SNAPSHOT_COLLECTION'
    env_config_path = 'CODEPACK_CONN_PATH'
    mss = None
    try:
        os.environ[env_source] = 'mongodb'
        os.environ[env_db] = 'test'
        os.environ[env_collection] = 'snapshot'
        os.environ[env_config_path] = 'config/test_conn.ini'
        mss = default.get_storage_instance('code_snapshot', 'snapshot_service')
        assert hasattr(mss, 'mongodb')
    finally:
        for env in [env_source, env_db, env_collection, env_config_path]:
            if env in os.environ:
                os.environ.pop(env, None)
        if mss is not None and not mss.mongodb.closed:
            mss.mongodb.close()


def test_default_memory_delivery_service_with_os_env(default):
    config = Config()
    with pytest.raises(AssertionError):
        config.get_storage_config('delivery')
    env_source = 'CODEPACK_DELIVERY_SOURCE'
    try:
        os.environ[env_source] = 'memory'
        mds = default.get_storage_instance('delivery', 'delivery_service')
        assert hasattr(mds, 'memory')
    finally:
        if env_source in os.environ:
            os.environ.pop(env_source, None)


def test_default_file_delivery_service_with_os_env(default):
    config = Config()
    with pytest.raises(AssertionError):
        config.get_storage_config('delivery')
    env_source = 'CODEPACK_DELIVERY_SOURCE'
    env_path = 'CODEPACK_DELIVERY_PATH'
    try:
        os.environ[env_source] = 'file'
        os.environ[env_path] = 'tmp/'
        fds = default.get_storage_instance('delivery', 'delivery_service')
        assert hasattr(fds, 'path')
        assert fds.path == 'tmp/'
    finally:
        for env in [env_source, env_path]:
            if env in os.environ:
                os.environ.pop(env, None)


def test_default_mongo_delivery_service_with_os_env(default):
    config = Config()
    with pytest.raises(AssertionError):
        config.get_storage_config('delivery')
    env_source = 'CODEPACK_DELIVERY_SOURCE'
    env_db = 'CODEPACK_DELIVERY_DB'
    env_collection = 'CODEPACK_DELIVERY_COLLECTION'
    env_config_path = 'CODEPACK_CONN_PATH'
    mds = None
    try:
        os.environ[env_source] = 'mongodb'
        os.environ[env_db] = 'test'
        os.environ[env_collection] = 'delivery'
        os.environ[env_config_path] = 'config/test_conn.ini'
        mds = default.get_storage_instance('delivery', 'delivery_service')
        assert hasattr(mds, 'mongodb')
    finally:
        for env in [env_source, env_db, env_collection, env_config_path]:
            if env in os.environ:
                os.environ.pop(env, None)
        if mds is not None and not mds.mongodb.closed:
            mds.mongodb.close()


def test_default_memory_code_storage_service_with_os_env(default):
    config = Config()
    with pytest.raises(AssertionError):
        config.get_storage_config('code')
    env_source = 'CODEPACK_CODE_SOURCE'
    try:
        os.environ[env_source] = 'memory'
        mss = default.get_storage_instance('code', 'storage_service')
        assert hasattr(mss, 'memory')
        assert mss.item_type == Code
    finally:
        if env_source in os.environ:
            os.environ.pop(env_source, None)


def test_default_file_code_storage_service_with_os_env(default):
    config = Config()
    with pytest.raises(AssertionError):
        config.get_storage_config('code')
    env_source = 'CODEPACK_CODE_SOURCE'
    env_path = 'CODEPACK_CODE_PATH'
    try:
        os.environ[env_source] = 'file'
        os.environ[env_path] = 'tmp/'
        fss = default.get_storage_instance('code', 'storage_service')
        assert hasattr(fss, 'path')
        assert fss.path == 'tmp/'
        assert fss.item_type == Code
    finally:
        for env in [env_source, env_path]:
            if env in os.environ:
                os.environ.pop(env, None)


def test_default_mongo_code_storage_service_with_os_env(default):
    config = Config()
    with pytest.raises(AssertionError):
        config.get_storage_config('code')
    env_source = 'CODEPACK_CODE_SOURCE'
    env_db = 'CODEPACK_CODE_DB'
    env_collection = 'CODEPACK_CODE_COLLECTION'
    env_config_path = 'CODEPACK_CONN_PATH'
    mss = None
    try:
        os.environ[env_source] = 'mongodb'
        os.environ[env_db] = 'test'
        os.environ[env_collection] = 'codes'
        os.environ[env_config_path] = 'config/test_conn.ini'
        mss = default.get_storage_instance('code', 'storage_service')
        assert hasattr(mss, 'mongodb')
        assert mss.item_type == Code
    finally:
        for env in [env_source, env_db, env_collection, env_config_path]:
            if env in os.environ:
                os.environ.pop(env, None)
        if mss is not None and not mss.mongodb.closed:
            mss.mongodb.close()


def test_if_default_services_have_single_instance_for_each_service(default, testdir_snapshot_service):
    os.environ['CODEPACK_DELIVERY_SOURCE'] = 'memory'
    os.environ['CODEPACK_CODE_SNAPSHOT_SOURCE'] = 'file'
    os.environ['CODEPACK_CODE_SNAPSHOT_PATH'] = testdir_snapshot_service
    os.environ['CODEPACK_CODE_SOURCE'] = 'mongodb'
    os.environ['CODEPACK_CODE_DB'] = 'test'
    os.environ['CODEPACK_CODE_COLLECTION'] = 'codes'
    os.environ['CODEPACK_CONN_PATH'] = 'config/test_conn.ini'
    try:
        code1 = Code(add2)
        code2 = Code(add3)
        assert isinstance(code1.service['delivery'], MemoryDeliveryService)
        assert isinstance(code1.service['snapshot'], FileSnapshotService)
        assert isinstance(code1.service['storage'], MongoStorageService)
        assert isinstance(code2.service['delivery'], MemoryDeliveryService)
        assert isinstance(code2.service['snapshot'], FileSnapshotService)
        assert isinstance(code2.service['storage'], MongoStorageService)

        assert (code1.service['delivery']) == (code2.service['delivery'])
        assert (code1.service['snapshot']) == (code2.service['snapshot'])
        assert (code1.service['storage']) == (code2.service['storage'])

        assert id(code1.service['delivery']) == id(code2.service['delivery'])
        assert id(code1.service['snapshot']) == id(code2.service['snapshot'])
        assert id(code1.service['storage']) == id(code2.service['storage'])
    finally:
        for env in ['CODEPACK_DELIVERY_SOURCE',
                    'CODEPACK_CODE_SNAPSHOT_SOURCE', 'CODEPACK_CODE_SNAPSHOT_PATH',
                    'CODEPACK_CODE_SOURCE', 'CODEPACK_CODE_DB', 'CODEPACK_CODE_COLLECTION',
                    'CODEPACK_CONN_PATH']:
            os.environ.pop(env, None)
