from codepack import Code, Config, Default, DeliveryService, SnapshotService, StorageService
from codepack.storages import MemoryStorage, FileStorage, MongoStorage
from tests import *
from unittest.mock import patch
import pytest
import os


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


def test_default_config_with_os_env(default_os_env):
    code = Code(add2)
    assert code(1, 2) == 3


def test_config_path():
    code = Code(add2, config_path='config/test.ini')
    ret = code(1, 2)
    assert ret == 3


def test_config_path_priority():
    try:
        config = Config()
        _config = config.get_config(section='logger', ignore_error=True)
        assert _config == {'log_dir': 'logs', 'name': 'default-logger',
                           'config_path': os.path.join(config.get_default_config_dir(), 'logging.json')}
        with pytest.raises(AssertionError):
            Config(config_path='test.ini')
        os.environ['CODEPACK_CONFIG_DIR'] = 'config'
        config = Config(config_path='test.ini')
        assert config.config_path == os.path.join('config', 'test.ini')
        _config = config.get_config(section='logger')
        ref = {'name': 'default-logger', 'config_path': 'logging.json', 'log_dir': 'logs'}
        assert _config == ref
        os.environ['CODEPACK_CONFIG_PATH'] = 'codepack.ini'
        _config = config.get_config(section='logger')
        assert _config == ref
        _config = config.get_config(section='logger', config_path='test.ini')
        assert _config == ref
    finally:
        os.environ.pop('CODEPACK_CONFIG_PATH', None)
        os.environ.pop('CODEPACK_CONFIG_DIR', None)


def test_config_get_value_priority():
    try:
        config = Config()
        _config = config.get_config(section='logger')
        assert _config == {'name': 'default-logger', 'log_dir': 'logs',
                           'config_path': os.path.join(config.get_default_config_dir(), 'logging.json')}
        default_value = Config.collect_value(section='logger', key='name', config=dict())
        assert default_value == 'default-logger'
        name = Config.collect_value(section='logger', key='name', config=_config)
        assert name == 'default-logger'
        os.environ['CODEPACK_LOGGER_NAME'] = 'test-logger'
        name = Config.collect_value(section='logger', key='name', config=_config)
        assert name == 'test-logger'
        os.environ.pop('CODEPACK_LOGGER_NAME')
        os.environ['CODEPACK_CONFIG_PATH'] = 'config/test.ini'
        os.environ['CODEPACK_CONFIG_DIR'] = 'config'
        config = Config()
        _config = config.get_config(section='logger')
        assert _config == {'name': 'default-logger',
                           'config_path': os.path.join('config', 'logging.json'),
                           'log_dir': 'logs'}
        name = Config.collect_value(section='logger', key='name', config=_config)
        assert name == 'default-logger'
        os.environ['CODEPACK_LOGGER_NAME'] = 'test-logger'
        name = Config.collect_value(section='logger', key='name', config=_config)
        assert name == 'test-logger'
    finally:
        os.environ.pop('CODEPACK_LOGGER_NAME', None)
        os.environ.pop('CODEPACK_CONFIG_PATH', None)
        os.environ.pop('CODEPACK_CONFIG_DIR', None)


def test_get_storage_config_priority():
    try:
        os.environ['CODEPACK_CONFIG_DIR'] = 'config'
        config = Config('test.ini')
        storage_config = config.get_storage_config('worker')
        ref = {'group_id': 'codepack_worker_test', 'interval': '5', 'kafka': {'bootstrap_servers': 'localhost:9092'},
               'script_path': 'scripts/run_snapshot.py', 'source': 'kafka', 'supervisor': 'http://localhost:8000',
               'topic': 'test', 'logger': 'worker-logger'}
        assert storage_config == ref
        os.environ['CODEPACK_WORKER_SCRIPT'] = 'test_script.py'
        os.environ['CODEPACK_WORKER_TOPIC'] = 'test2'
        storage_config = config.get_storage_config('worker')
        assert storage_config == ref
        os.environ['CODEPACK_WORKER_TEST_KEY'] = 'test'
        storage_config = config.get_storage_config('worker')
        assert storage_config == ref
    finally:
        os.environ.pop('CODEPACK_CONFIG_DIR', None)
        os.environ.pop('CODEPACK_WORKER_SCRIPT', None)
        os.environ.pop('CODEPACK_WORKER_TOPIC', None)
        os.environ.pop('CODEPACK_WORKER_TEST_KEY', None)


def test_default_memory_code_snapshot_service_with_os_env(fake_mongodb):
    mss = None
    try:
        config = Config()
        storage_config = config.get_storage_config('code_snapshot')
        assert storage_config == {'source': 'memory'}
        os.environ['CODEPACK_CODESNAPSHOT_SOURCE'] = 'mongodb'
        os.environ['CODEPACK_CODESNAPSHOT_DB'] = 'test_db'
        os.environ['CODEPACK_CODESNAPSHOT_COLLECTION'] = 'test_collection'
        mss = Default.get_service('code_snapshot', 'snapshot_service')
        assert hasattr(mss.storage, 'mongodb')
        assert mss.storage.db == 'test_db'
        assert mss.storage.collection == 'test_collection'
    finally:
        if mss is not None and not mss.storage.mongodb.closed():
            mss.storage.mongodb.close()
        os.environ.pop('CODEPACK_CODESNAPSHOT_SOURCE', None)
        os.environ.pop('CODEPACK_CODESNAPSHOT_DB', None)
        os.environ.pop('CODEPACK_CODESNAPSHOT_COLLECTION', None)


def test_default_file_code_snapshot_service_with_os_env():
    config = Config()
    storage_config = config.get_storage_config('code_snapshot')
    assert storage_config == {'source': 'memory'}
    env_source = 'CODEPACK_CODESNAPSHOT_SOURCE'
    env_path = 'CODEPACK_CODESNAPSHOT_PATH'
    try:
        os.environ[env_source] = 'file'
        os.environ[env_path] = 'tmp/'
        fss = Default.get_service('code_snapshot', 'snapshot_service')
        assert hasattr(fss.storage, 'path')
        assert fss.storage.path == 'tmp/'
    finally:
        for env in [env_source, env_path]:
            os.environ.pop(env, None)


def test_default_mongo_code_snapshot_service_with_os_env():
    config = Config()
    storage_config = config.get_storage_config('code_snapshot')
    assert storage_config == {'source': 'memory'}
    env_source = 'CODEPACK_CODESNAPSHOT_SOURCE'
    env_db = 'CODEPACK_CODESNAPSHOT_DB'
    env_collection = 'CODEPACK_CODESNAPSHOT_COLLECTION'
    mss = None
    try:
        os.environ[env_source] = 'mongodb'
        os.environ[env_db] = 'test'
        os.environ[env_collection] = 'snapshot'
        mss = Default.get_service('code_snapshot', 'snapshot_service')
    finally:
        if mss is not None and not mss.storage.mongodb.closed():
            mss.storage.mongodb.close()
        for env in [env_source, env_db, env_collection]:
            os.environ.pop(env, None)
    assert hasattr(mss.storage, 'mongodb')


def test_default_memory_delivery_service_with_os_env():
    config = Config()
    storage_config = config.get_storage_config('delivery')
    assert storage_config == {'source': 'memory'}
    env_source = 'CODEPACK_DELIVERY_SOURCE'
    try:
        os.environ[env_source] = 'memory'
        mds = Default.get_service('delivery', 'delivery_service')
        assert isinstance(mds.storage, MemoryStorage)
    finally:
        for env in [env_source]:
            os.environ.pop(env, None)


def test_default_file_delivery_service_with_os_env():
    config = Config()
    storage_config = config.get_storage_config('delivery')
    assert storage_config == {'source': 'memory'}
    env_source = 'CODEPACK_DELIVERY_SOURCE'
    env_path = 'CODEPACK_DELIVERY_PATH'
    try:
        os.environ[env_source] = 'file'
        os.environ[env_path] = 'tmp/'
        fds = Default.get_service('delivery', 'delivery_service')
        assert hasattr(fds.storage, 'path')
        assert fds.storage.path == 'tmp/'
    finally:
        for env in [env_source, env_path]:
            os.environ.pop(env, None)


def test_default_mongo_delivery_service_with_os_env():
    config = Config()
    storage_config = config.get_storage_config('delivery')
    assert storage_config == {'source': 'memory'}
    env_source = 'CODEPACK_DELIVERY_SOURCE'
    env_db = 'CODEPACK_DELIVERY_DB'
    env_collection = 'CODEPACK_DELIVERY_COLLECTION'
    mds = None
    try:
        os.environ[env_source] = 'mongodb'
        os.environ[env_db] = 'test'
        os.environ[env_collection] = 'delivery'
        mds = Default.get_service('delivery', 'delivery_service')
        assert hasattr(mds.storage, 'mongodb')
        assert mds.storage.db == 'test'
        assert mds.storage.collection == 'delivery'
    finally:
        for env in [env_source, env_db, env_collection]:
            os.environ.pop(env, None)
        if mds is not None and not mds.storage.mongodb.closed():
            mds.storage.mongodb.close()


def test_default_memory_code_storage_service_with_os_env():
    config = Config()
    storage_config = config.get_storage_config('code')
    assert storage_config == {'source': 'memory'}
    env_source = 'CODEPACK_CODE_SOURCE'
    try:
        os.environ[env_source] = 'memory'
        mss = Default.get_service('code', 'storage_service')
        assert hasattr(mss.storage, 'memory')
        assert mss.storage.item_type == Code
    finally:
        if env_source in os.environ:
            os.environ.pop(env_source, None)


def test_default_file_code_storage_service_with_os_env():
    config = Config()
    storage_config = config.get_storage_config('code')
    assert storage_config == {'source': 'memory'}
    env_source = 'CODEPACK_CODE_SOURCE'
    env_path = 'CODEPACK_CODE_PATH'
    try:
        os.environ[env_source] = 'file'
        os.environ[env_path] = 'tmp/'
        fss = Default.get_service('code', 'storage_service')
        assert hasattr(fss.storage, 'path')
        assert fss.storage.path == 'tmp/'
        assert fss.storage.item_type == Code
    finally:
        for env in [env_source, env_path]:
            if env in os.environ:
                os.environ.pop(env, None)


def test_default_mongo_code_storage_service_with_os_env():
    config = Config()
    storage_config = config.get_storage_config('code')
    assert storage_config == {'source': 'memory'}
    env_source = 'CODEPACK_CODE_SOURCE'
    env_db = 'CODEPACK_CODE_DB'
    env_collection = 'CODEPACK_CODE_COLLECTION'
    mss = None
    try:
        os.environ[env_source] = 'mongodb'
        os.environ[env_db] = 'test'
        os.environ[env_collection] = 'codes'
        mss = Default.get_service('code', 'storage_service')
        assert hasattr(mss.storage, 'mongodb')
        assert mss.storage.item_type == Code
        assert mss.storage.db == 'test'
        assert mss.storage.collection == 'codes'
    finally:
        for env in [env_source, env_db, env_collection]:
            os.environ.pop(env, None)
        if mss is not None and not mss.storage.mongodb.closed():
            mss.storage.mongodb.close()


def test_if_default_services_have_single_instance_for_each_service(testdir_snapshot_service):
    os.environ['CODEPACK_DELIVERY_SOURCE'] = 'memory'
    os.environ['CODEPACK_CODESNAPSHOT_SOURCE'] = 'file'
    os.environ['CODEPACK_CODESNAPSHOT_PATH'] = testdir_snapshot_service
    os.environ['CODEPACK_CODE_SOURCE'] = 'mongodb'
    os.environ['CODEPACK_CODE_DB'] = 'test'
    os.environ['CODEPACK_CODE_COLLECTION'] = 'codes'
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
                    'CODEPACK_CODE_SOURCE', 'CODEPACK_CODE_DB', 'CODEPACK_CODE_COLLECTION']:
            os.environ.pop(env, None)


def test_config_dir():
    path = Config.collect_value(section='?', key='path', config={'path': 'config/test.ini'})
    assert path == 'config/test.ini'
    with pytest.raises(AssertionError):
        Config.collect_value(section='conn', key='path', config={'path': 'test.ini'})
    try:
        os.environ['CODEPACK_CONFIG_DIR'] = 'config'
        path = Config.collect_value(section='conn', key='path', config={'path': 'test.ini'})
        assert path == os.path.join('config', 'test.ini')
    finally:
        os.environ.pop('CODEPACK_CONFIG_DIR', None)


@patch('codepack.utils.config.config.Config.parse_config')
def test_get_config_without_anything(parse_config):
    config = Config()
    ret = config.get_config('worker')
    assert ret is not None
    arg_list = parse_config.call_args_list
    assert len(arg_list) == 2
    args, kwargs = arg_list[0]
    assert kwargs == {'section': 'worker', 'config_path': config.get_default_config_path()}
    args, kwargs = arg_list[1]
    assert kwargs == {'section': 'worker',
                      'config_path': config.get_default_config_path()}


@patch('codepack.utils.config.config.Config.parse_config')
def test_get_config_without_anything_but_os_env(parse_config):
    os.environ['CODEPACK_CONFIG_PATH'] = 'config/test.ini'
    try:
        config = Config()
        ret = config.get_config('worker')
        assert ret is not None
        arg_list = parse_config.call_args_list
        assert len(arg_list) == 2
        args, kwargs = arg_list[0]
        assert kwargs == {'section': 'worker', 'config_path': 'config/test.ini'}
        args, kwargs = arg_list[1]
        assert kwargs == {'section': 'worker',
                          'config_path': config.get_default_config_path()}
    finally:
        os.environ.pop('CODEPACK_CONFIG_PATH')


@patch('codepack.utils.config.config.Config.parse_config')
def test_get_config_without_anything_but_method_argument(parse_config):
    config = Config()
    ret = config.get_config('worker', config_path='config/test.ini')
    assert ret is not None
    parse_config.assert_called_once_with(section='worker', config_path='config/test.ini')


@patch('codepack.utils.config.config.Config.parse_config')
def test_get_config_with_constructor_argument(parse_config):
    config = Config(config_path='config/test.ini')
    ret = config.get_config('worker')
    assert ret is not None
    parse_config.assert_called_once_with(section='worker', config_path='config/test.ini')


@patch('codepack.utils.config.config.Config.parse_config')
def test_get_config_with_constructor_argument_and_os_env(parse_config):
    try:
        os.environ['CODEPACK_CONFIG_PATH'] = 'config/codepack.ini'
        config = Config(config_path='config/test.ini')
        ret = config.get_config('worker')
        assert ret is not None
        parse_config.assert_called_once_with(section='worker', config_path='config/test.ini')
    finally:
        os.environ.pop('CODEPACK_CONFIG_PATH')


@patch('codepack.utils.config.config.Config.parse_config')
def test_get_config_with_constructor_argument_and_method_argument(parse_config):
    config = Config(config_path='config/test.ini')
    ret = config.get_config('worker', config_path='config/codepack.ini')
    assert ret is not None
    parse_config.assert_called_once_with(section='worker', config_path='config/codepack.ini')


@patch('codepack.utils.config.config.Config.parse_config')
def test_get_config_with_method_argument_and_os_env(parse_config):
    try:
        os.environ['CODEPACK_CONFIG_PATH'] = 'config/codepack.ini'
        config = Config()
        ret = config.get_config('worker', config_path='config/test.ini')
        assert ret is not None
        parse_config.assert_called_once_with(section='worker', config_path='config/test.ini')
    finally:
        os.environ.pop('CODEPACK_CONFIG_PATH')


def test_collect_values_without_anything():
    os_envs = {'CODEPACK_WORKER_LOGGER': 'dummy-logger',
               'CODEPACK_WORKER_DUMMY': 'dummy_value'}
    try:
        for k, v in os_envs.items():
            os.environ[k] = v
        config = Config()
        ret = config.get_config('worker')
        assert ret == {'dummy': 'dummy_value',
                       'background': 'True',
                       'interval': '1',
                       'logger': 'dummy-logger',
                       'source': 'memory',
                       'topic': 'codepack',
                       'script_path': os.path.join(config.get_default_config_dir(), 'scripts/run_snapshot.py')}
    finally:
        for k in os_envs.keys():
            os.environ.pop(k, None)


def test_collect_values_without_anything_but_os_env():
    os_envs = {'CODEPACK_CONFIG_PATH': 'config/test.ini',
               'CODEPACK_WORKER_LOGGER': 'dummy-logger',
               'CODEPACK_WORKER_DUMMY': 'dummy_value'}
    try:
        for k, v in os_envs.items():
            os.environ[k] = v
        config = Config()
        ret = config.get_config('worker')
        assert ret == {'background': 'True', 'dummy': 'dummy_value',
                       'group_id': 'codepack_worker_test',
                       'interval': '5',
                       'logger': 'dummy-logger',
                       'script_path': 'scripts/run_snapshot.py',
                       'source': 'kafka',
                       'supervisor': 'http://localhost:8000',
                       'topic': 'test'}
    finally:
        for k in os_envs.keys():
            os.environ.pop(k, None)


def test_collect_values_without_anything_but_method_argument():
    os_envs = {'CODEPACK_WORKER_LOGGER': 'dummy-logger',
               'CODEPACK_WORKER_DUMMY': 'dummy_value'}
    try:
        for k, v in os_envs.items():
            os.environ[k] = v
        config = Config()
        ret = config.get_config('worker', config_path='config/test.ini')
        assert ret == {'group_id': 'codepack_worker_test',
                       'interval': '5',
                       'logger': 'worker-logger',
                       'script_path': 'scripts/run_snapshot.py',
                       'source': 'kafka',
                       'supervisor': 'http://localhost:8000',
                       'topic': 'test'}
    finally:
        for k in os_envs.keys():
            os.environ.pop(k, None)


def test_collect_values_with_constructor_argument():
    os_envs = {'CODEPACK_WORKER_LOGGER': 'dummy-logger',
               'CODEPACK_WORKER_DUMMY': 'dummy_value'}
    try:
        for k, v in os_envs.items():
            os.environ[k] = v
        config = Config(config_path='config/test.ini')
        ret = config.get_config('worker')
        assert ret == {'group_id': 'codepack_worker_test',
                       'interval': '5',
                       'logger': 'worker-logger',
                       'script_path': 'scripts/run_snapshot.py',
                       'source': 'kafka',
                       'supervisor': 'http://localhost:8000',
                       'topic': 'test'}
    finally:
        for k in os_envs.keys():
            os.environ.pop(k, None)


def test_get_storage_config_with_os_env():
    try:
        os.environ['CODEPACK_CONFIG_PATH'] = 'config/test.ini'
        config = Config()
        ret = config.get_storage_config('scheduler')
        ref = {'collection': 'scheduler', 'db': 'codepack', 'source': 'mongodb',
               'supervisor': 'http://localhost:8000',
               'mongodb': {'host': 'localhost', 'port': '27017'}}
        assert ret == ref
        os.environ['CODEPACK_SCHEDULER_DB'] = 'test'
        ret = config.get_storage_config('scheduler')
        ref['db'] = 'test'
        assert ret == ref
        os.environ['CODEPACK_MONGODB_HOST'] = '127.0.0.1'
        os.environ['CODEPACK_MONGODB_USER'] = 'admin'
        ret = config.get_storage_config('scheduler')
        ref['mongodb']['host'] = '127.0.0.1'
        ref['mongodb']['user'] = 'admin'
        assert ret == ref
    finally:
        os.environ.pop('CODEPACK_CONFIG_PATH', None)
        os.environ.pop('CODEPACK_SCHEDULER_DB', None)
        os.environ.pop('CODEPACK_MONGODB_HOST', None)
        os.environ.pop('CODEPACK_MONGODB_USER', None)
