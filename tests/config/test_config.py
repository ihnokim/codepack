from codepack import Code
from tests import *
from codepack.config import Config
import pytest
import os
from codepack.service import DeliveryService, SnapshotService, StorageService
from codepack.storage import MemoryStorage, FileStorage, MongoStorage


def test_no_config():
    code = Code(add2)
    assert 'delivery' in code.service and isinstance(code.service['delivery'], DeliveryService)
    assert 'snapshot' in code.service and isinstance(code.service['snapshot'], SnapshotService)
    assert 'storage' in code.service and isinstance(code.service['storage'], StorageService)
    assert isinstance(code.service['delivery'].storage, MemoryStorage)
    assert isinstance(code.service['snapshot'].storage, MemoryStorage)
    assert isinstance(code.service['storage'].storage, MemoryStorage)


def test_some_os_env(fake_mongodb):
    try:
        os.environ['CODEPACK_DELIVERY_SOURCE'] = 'mongodb'
        os.environ['CODEPACK_DELIVERY_DB'] = 'codepack'
        os.environ['CODEPACK_DELIVERY_COLLECTION'] = 'test'
        with pytest.raises(AssertionError):
            Code(add2)
        os.environ['CODEPACK_CONN_PATH'] = 'config/test_conn.ini'
        code = Code(add2)
        assert 'delivery' in code.service and isinstance(code.service['delivery'], DeliveryService)
        assert 'snapshot' in code.service and isinstance(code.service['snapshot'], SnapshotService)
        assert 'storage' in code.service and isinstance(code.service['storage'], StorageService)
        assert isinstance(code.service['delivery'].storage, MongoStorage)
        assert isinstance(code.service['snapshot'].storage, MemoryStorage)
        assert isinstance(code.service['storage'].storage, MemoryStorage)
        assert code.service['delivery'].storage.db == 'codepack'
        assert code.service['delivery'].storage.collection == 'test'
    finally:
        os.environ.pop('CODEPACK_DELIVERY_SOURCE', None)
        os.environ.pop('CODEPACK_DELIVERY_DB', None)
        os.environ.pop('CODEPACK_DELIVERY_COLLECTION', None)
        os.environ.pop('CODEPACK_CONN_PATH', None)


def test_default_config_with_os_env(default_os_env):
    code = Code(add2)
    assert code(1, 2) == 3


def test_config_path():
    os.environ['CODEPACK_CONFIG_DIR'] = 'config'
    try:
        code = Code(add2, config_path='config/test.ini')
        ret = code(1, 2)
    finally:
        os.environ.pop('CODEPACK_CONFIG_DIR')
    assert ret == 3


def test_config_path_priority():
    config = Config()
    _config = config.get_config(section='logger', ignore_error=True)
    assert _config == {'log_dir': 'logs', 'name': 'default-logger'}
    with pytest.raises(AssertionError):
        Config(config_path='test.ini')
    os.environ['CODEPACK_CONFIG_DIR'] = 'config'
    config = Config(config_path='test.ini')
    assert config.config_path == os.path.join('config', 'test.ini')
    _config = config.get_config(section='logger')
    assert _config == {'name': 'default-logger', 'path': 'logging.json', 'log_dir': 'logs'}
    os.environ['CODEPACK_CONFIG_PATH'] = 'codepack.ini'
    _config = config.get_config(section='logger')
    assert _config == {'name': 'error-logger', 'path': 'logging.json', 'log_dir': 'logs'}
    _config = config.get_config(section='logger', config_path='test.ini')
    assert _config == {'name': 'default-logger', 'path': 'logging.json', 'log_dir': 'logs'}
    os.environ.pop('CODEPACK_CONFIG_PATH')
    os.environ.pop('CODEPACK_CONFIG_DIR')


def test_config_get_value_priority():
    config = Config('config/test.ini')
    _config = config.get_config(section='logger')
    assert _config == {'name': 'default-logger', 'path': 'logging.json', 'log_dir': 'logs'}
    default_value = Config.get_value('logger', 'name')
    assert default_value == 'default-logger'
    name = Config.get_value('logger', 'name', _config)
    assert name == 'default-logger'
    os.environ['CODEPACK_LOGGER_NAME'] = 'test-logger'
    name = Config.get_value('logger', 'name', _config)
    assert name == 'test-logger'
    os.environ.pop('CODEPACK_LOGGER_NAME')
    os.environ['CODEPACK_CONFIG_PATH'] = 'config/test.ini'
    config = Config()
    _config = config.get_config(section='logger')
    assert _config == {'name': 'default-logger', 'path': 'logging.json', 'log_dir': 'logs'}
    name = Config.get_value('logger', 'name', _config)
    assert name == 'default-logger'
    os.environ['CODEPACK_LOGGER_NAME'] = 'test-logger'
    name = Config.get_value('logger', 'name', _config)
    assert name == 'test-logger'
    os.environ.pop('CODEPACK_LOGGER_NAME')
    os.environ.pop('CODEPACK_CONFIG_PATH')


def test_get_storage_config_priority():
    os.environ['CODEPACK_CONFIG_DIR'] = 'config'
    config = Config('test.ini')
    storage_config = config.get_storage_config('worker')
    ref = {'group_id': 'codepack_worker', 'interval': '1', 'kafka': {'bootstrap_servers': 'localhost:9092'},
           'path': 'scripts', 'script': 'run_snapshot.py', 'source': 'kafka', 'supervisor': 'http://localhost:8000',
           'topic': 'test', 'logger': 'worker-logger'}
    assert storage_config == ref
    os.environ['CODEPACK_WORKER_SCRIPT'] = 'test_script.py'
    os.environ['CODEPACK_WORKER_TOPIC'] = 'test2'
    storage_config = config.get_storage_config('worker')
    ref = {'group_id': 'codepack_worker', 'interval': '1', 'kafka': {'bootstrap_servers': 'localhost:9092'},
           'path': 'scripts', 'script': 'test_script.py', 'source': 'kafka', 'supervisor': 'http://localhost:8000',
           'topic': 'test2', 'logger': 'worker-logger'}
    assert storage_config == ref
    os.environ['CODEPACK_WORKER_TEST_KEY'] = 'test'
    storage_config = config.get_storage_config('worker')
    ref = {'group_id': 'codepack_worker', 'interval': '1', 'kafka': {'bootstrap_servers': 'localhost:9092'},
           'path': 'scripts', 'script': 'test_script.py', 'source': 'kafka', 'supervisor': 'http://localhost:8000',
           'topic': 'test2', 'test_key': 'test', 'logger': 'worker-logger'}
    assert storage_config == ref
    os.environ.pop('CODEPACK_CONFIG_DIR')
    os.environ.pop('CODEPACK_WORKER_SCRIPT')
    os.environ.pop('CODEPACK_WORKER_TOPIC')
    os.environ.pop('CODEPACK_WORKER_TEST_KEY')


def test_default_memory_code_snapshot_service_with_os_env(default):
    config = Config()
    storage_config = config.get_storage_config('code_snapshot')
    assert storage_config == {'source': 'memory'}
    env_source = 'CODEPACK_CODESNAPSHOT_SOURCE'
    try:
        os.environ[env_source] = 'memory'
        mss = default.get_service('code_snapshot', 'snapshot_service')
        assert hasattr(mss.storage, 'memory')
    finally:
        if env_source in os.environ:
            os.environ.pop(env_source, None)


def test_default_file_code_snapshot_service_with_os_env(default):
    config = Config()
    storage_config = config.get_storage_config('code_snapshot')
    assert storage_config == {'source': 'memory'}
    env_source = 'CODEPACK_CODESNAPSHOT_SOURCE'
    env_path = 'CODEPACK_CODESNAPSHOT_PATH'
    try:
        os.environ[env_source] = 'file'
        os.environ[env_path] = 'tmp/'
        fss = default.get_service('code_snapshot', 'snapshot_service')
        assert hasattr(fss.storage, 'path')
        assert fss.storage.path == 'tmp/'
    finally:
        for env in [env_source, env_path]:
            if env in os.environ:
                os.environ.pop(env, None)


def test_default_mongo_code_snapshot_service_with_os_env(default):
    config = Config()
    storage_config = config.get_storage_config('code_snapshot')
    assert storage_config == {'source': 'memory'}
    env_source = 'CODEPACK_CODESNAPSHOT_SOURCE'
    env_db = 'CODEPACK_CODESNAPSHOT_DB'
    env_collection = 'CODEPACK_CODESNAPSHOT_COLLECTION'
    env_config_path = 'CODEPACK_CONN_PATH'
    mss = None
    try:
        os.environ[env_source] = 'mongodb'
        os.environ[env_db] = 'test'
        os.environ[env_collection] = 'snapshot'
        os.environ[env_config_path] = 'config/test_conn.ini'
        mss = default.get_service('code_snapshot', 'snapshot_service')
    finally:
        for env in [env_source, env_db, env_collection, env_config_path]:
            if env in os.environ:
                os.environ.pop(env, None)
        if mss is not None and not mss.storage.mongodb.closed():
            mss.storage.mongodb.close()
    assert hasattr(mss.storage, 'mongodb')


def test_default_memory_delivery_service_with_os_env(default):
    config = Config()
    storage_config = config.get_storage_config('delivery')
    assert storage_config == {'source': 'memory'}
    env_source = 'CODEPACK_DELIVERY_SOURCE'
    try:
        os.environ[env_source] = 'memory'
        mds = default.get_service('delivery', 'delivery_service')
        assert hasattr(mds.storage, 'memory')
    finally:
        if env_source in os.environ:
            os.environ.pop(env_source, None)


def test_default_file_delivery_service_with_os_env(default):
    config = Config()
    storage_config = config.get_storage_config('delivery')
    assert storage_config == {'source': 'memory'}
    env_source = 'CODEPACK_DELIVERY_SOURCE'
    env_path = 'CODEPACK_DELIVERY_PATH'
    try:
        os.environ[env_source] = 'file'
        os.environ[env_path] = 'tmp/'
        fds = default.get_service('delivery', 'delivery_service')
        assert hasattr(fds.storage, 'path')
        assert fds.storage.path == 'tmp/'
    finally:
        for env in [env_source, env_path]:
            if env in os.environ:
                os.environ.pop(env, None)


def test_default_mongo_delivery_service_with_os_env(default):
    config = Config()
    storage_config = config.get_storage_config('delivery')
    assert storage_config == {'source': 'memory'}
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
        mds = default.get_service('delivery', 'delivery_service')
    finally:
        for env in [env_source, env_db, env_collection, env_config_path]:
            if env in os.environ:
                os.environ.pop(env, None)
        if mds is not None and not mds.storage.mongodb.closed():
            mds.storage.mongodb.close()
    assert hasattr(mds.storage, 'mongodb')


def test_default_memory_code_storage_service_with_os_env(default):
    config = Config()
    storage_config = config.get_storage_config('code')
    assert storage_config == {'source': 'memory'}
    env_source = 'CODEPACK_CODE_SOURCE'
    try:
        os.environ[env_source] = 'memory'
        mss = default.get_service('code', 'storage_service')
        assert hasattr(mss.storage, 'memory')
        assert mss.storage.item_type == Code
    finally:
        if env_source in os.environ:
            os.environ.pop(env_source, None)


def test_default_file_code_storage_service_with_os_env(default):
    config = Config()
    storage_config = config.get_storage_config('code')
    assert storage_config == {'source': 'memory'}
    env_source = 'CODEPACK_CODE_SOURCE'
    env_path = 'CODEPACK_CODE_PATH'
    try:
        os.environ[env_source] = 'file'
        os.environ[env_path] = 'tmp/'
        fss = default.get_service('code', 'storage_service')
        assert hasattr(fss.storage, 'path')
        assert fss.storage.path == 'tmp/'
        assert fss.storage.item_type == Code
    finally:
        for env in [env_source, env_path]:
            if env in os.environ:
                os.environ.pop(env, None)


def test_default_mongo_code_storage_service_with_os_env(default):
    config = Config()
    storage_config = config.get_storage_config('code')
    assert storage_config == {'source': 'memory'}
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
        mss = default.get_service('code', 'storage_service')
    finally:
        for env in [env_source, env_db, env_collection, env_config_path]:
            if env in os.environ:
                os.environ.pop(env, None)
        if mss is not None and not mss.storage.mongodb.closed():
            mss.storage.mongodb.close()
    assert hasattr(mss.storage, 'mongodb')
    assert mss.storage.item_type == Code


def test_if_default_services_have_single_instance_for_each_service(default, testdir_snapshot_service):
    os.environ['CODEPACK_DELIVERY_SOURCE'] = 'memory'
    os.environ['CODEPACK_CODESNAPSHOT_SOURCE'] = 'file'
    os.environ['CODEPACK_CODESNAPSHOT_PATH'] = testdir_snapshot_service
    os.environ['CODEPACK_CODE_SOURCE'] = 'mongodb'
    os.environ['CODEPACK_CODE_DB'] = 'test'
    os.environ['CODEPACK_CODE_COLLECTION'] = 'codes'
    os.environ['CODEPACK_CONN_PATH'] = 'config/test_conn.ini'
    try:
        code1 = Code(add2)
        code2 = Code(add3)
        assert isinstance(code1.service['delivery'], DeliveryService)
        assert isinstance(code1.service['snapshot'], SnapshotService)
        assert isinstance(code1.service['storage'], StorageService)
        assert isinstance(code2.service['delivery'], DeliveryService)
        assert isinstance(code2.service['snapshot'], SnapshotService)
        assert isinstance(code2.service['storage'], StorageService)

        assert isinstance(code1.service['delivery'].storage, MemoryStorage)
        assert isinstance(code1.service['snapshot'].storage, FileStorage)
        assert isinstance(code1.service['storage'].storage, MongoStorage)
        assert isinstance(code2.service['delivery'].storage, MemoryStorage)
        assert isinstance(code2.service['snapshot'].storage, FileStorage)
        assert isinstance(code2.service['storage'].storage, MongoStorage)

        assert (code1.service['delivery']) == (code2.service['delivery'])
        assert (code1.service['snapshot']) == (code2.service['snapshot'])
        assert (code1.service['storage']) == (code2.service['storage'])

        assert id(code1.service['delivery']) == id(code2.service['delivery'])
        assert id(code1.service['snapshot']) == id(code2.service['snapshot'])
        assert id(code1.service['storage']) == id(code2.service['storage'])
    finally:
        for env in ['CODEPACK_DELIVERY_SOURCE',
                    'CODEPACK_CODESNAPSHOT_SOURCE', 'CODEPACK_CODESNAPSHOT_PATH',
                    'CODEPACK_CODE_SOURCE', 'CODEPACK_CODE_DB', 'CODEPACK_CODE_COLLECTION',
                    'CODEPACK_CONN_PATH']:
            os.environ.pop(env, None)


def test_config_dir():
    path = Config.get_value(section='?', key='path', config={'path': 'config/test_conn.ini'})
    assert path == 'config/test_conn.ini'
    with pytest.raises(AssertionError):
        Config.get_value(section='conn', key='path', config={'path': 'test_conn.ini'})
    try:
        os.environ['CODEPACK_CONFIG_DIR'] = 'config'
        path = Config.get_value(section='conn', key='path', config={'path': 'test_conn.ini'})
        assert path == os.path.join('config', 'test_conn.ini')
    finally:
        os.environ.pop('CODEPACK_CONFIG_DIR', None)
