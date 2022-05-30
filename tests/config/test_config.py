from codepack import Code, Config, Default, DeliveryService, SnapshotService, StorageService
from codepack.storages import MemoryStorage, FileStorage, MongoStorage
from tests import add2, add3
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
        os.environ['CODEPACK__DELIVERY__SOURCE'] = 'mongodb'
        os.environ['CODEPACK__DELIVERY__DB'] = 'codepack'
        os.environ['CODEPACK__DELIVERY__COLLECTION'] = 'test'
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
        os.environ.pop('CODEPACK__DELIVERY__SOURCE', None)
        os.environ.pop('CODEPACK__DELIVERY__DB', None)
        os.environ.pop('CODEPACK__DELIVERY__COLLECTION', None)


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
        _config = config.get_config(section='logger')
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
        with pytest.raises(AssertionError):
            _ = Config.collect_value(section='logger', key='name', config=dict())
        name = Config.collect_value(section='logger', key='name', config=_config)
        assert name == 'default-logger'
        os.environ['CODEPACK__LOGGER__NAME'] = 'test-logger'
        name = Config.collect_value(section='logger', key='name', config=_config)
        assert name == 'test-logger'
        os.environ.pop('CODEPACK__LOGGER__NAME')
        os.environ['CODEPACK_CONFIG_PATH'] = 'config/test.ini'
        os.environ['CODEPACK_CONFIG_DIR'] = 'config'
        config = Config()
        _config = config.get_config(section='logger')
        assert _config == {'name': 'default-logger',
                           'config_path': 'logging.json',
                           'log_dir': 'logs'}
        name = Config.collect_value(section='logger', key='name', config=_config)
        assert name == 'default-logger'
        os.environ['CODEPACK__LOGGER__NAME'] = 'test-logger'
        name = Config.collect_value(section='logger', key='name', config=_config)
        assert name == 'test-logger'
    finally:
        os.environ.pop('CODEPACK__LOGGER__NAME', None)
        os.environ.pop('CODEPACK_CONFIG_PATH', None)
        os.environ.pop('CODEPACK_CONFIG_DIR', None)


def test_get_storage_config_priority():
    try:
        os.environ['CODEPACK_CONFIG_DIR'] = 'config'
        config = Config('test.ini')
        storage_config = config.get_storage_config('worker')
        ref = {'group_id': 'codepack_worker_test', 'interval': '5', 'kafka': {'bootstrap_servers': 'localhost:9092'},
               'script_path': 'scripts/run_snapshot.py', 'source': 'kafka', 'supervisor': 'http://localhost:8000',
               'topic': 'test', 'logger': 'worker-logger', 'background': 'True'}
        assert storage_config == ref
        os.environ['CODEPACK__WORKER__SCRIPT'] = 'test_script.py'
        os.environ['CODEPACK__WORKER__TOPIC'] = 'test2'
        storage_config = config.get_storage_config('worker')
        assert storage_config == ref
        os.environ['CODEPACK__WORKER__TEST_KEY'] = 'test'
        storage_config = config.get_storage_config('worker')
        assert storage_config == ref
    finally:
        os.environ.pop('CODEPACK_CONFIG_DIR', None)
        os.environ.pop('CODEPACK__WORKER__SCRIPT', None)
        os.environ.pop('CODEPACK__WORKER__TOPIC', None)
        os.environ.pop('CODEPACK__WORKER__TEST_KEY', None)


def test_default_memory_code_snapshot_service_with_os_env(fake_mongodb):
    mss = None
    try:
        config = Config()
        storage_config = config.get_storage_config('code_snapshot')
        assert storage_config == {'source': 'memory'}
        os.environ['CODEPACK__CODE_SNAPSHOT__SOURCE'] = 'mongodb'
        os.environ['CODEPACK__CODE_SNAPSHOT__DB'] = 'test_db'
        os.environ['CODEPACK__CODE_SNAPSHOT__COLLECTION'] = 'test_collection'
        mss = Default.get_service('code_snapshot', 'snapshot_service')
        assert hasattr(mss.storage, 'mongodb')
        assert mss.storage.db == 'test_db'
        assert mss.storage.collection == 'test_collection'
    finally:
        if mss is not None and not mss.storage.mongodb.closed():
            mss.storage.mongodb.close()
        os.environ.pop('CODEPACK__CODE_SNAPSHOT__SOURCE', None)
        os.environ.pop('CODEPACK__CODE_SNAPSHOT__DB', None)
        os.environ.pop('CODEPACK__CODE_SNAPSHOT__COLLECTION', None)


def test_default_file_code_snapshot_service_with_os_env():
    config = Config()
    storage_config = config.get_storage_config('code_snapshot')
    assert storage_config == {'source': 'memory'}
    env_source = 'CODEPACK__CODE_SNAPSHOT__SOURCE'
    env_path = 'CODEPACK__CODE_SNAPSHOT__PATH'
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
    env_source = 'CODEPACK__CODE_SNAPSHOT__SOURCE'
    env_db = 'CODEPACK__CODE_SNAPSHOT__DB'
    env_collection = 'CODEPACK__CODE_SNAPSHOT__COLLECTION'
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
    env_source = 'CODEPACK__DELIVERY__SOURCE'
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
    env_source = 'CODEPACK__DELIVERY__SOURCE'
    env_path = 'CODEPACK__DELIVERY__PATH'
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
    env_source = 'CODEPACK__DELIVERY__SOURCE'
    env_db = 'CODEPACK__DELIVERY__DB'
    env_collection = 'CODEPACK__DELIVERY__COLLECTION'
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
    env_source = 'CODEPACK__CODE__SOURCE'
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
    env_source = 'CODEPACK__CODE__SOURCE'
    env_path = 'CODEPACK__CODE__PATH'
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
    env_source = 'CODEPACK__CODE__SOURCE'
    env_db = 'CODEPACK__CODE__DB'
    env_collection = 'CODEPACK__CODE__COLLECTION'
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
    os.environ['CODEPACK__DELIVERY__SOURCE'] = 'memory'
    os.environ['CODEPACK__CODE_SNAPSHOT__SOURCE'] = 'file'
    os.environ['CODEPACK__CODE_SNAPSHOT__PATH'] = testdir_snapshot_service
    os.environ['CODEPACK__CODE__SOURCE'] = 'mongodb'
    os.environ['CODEPACK__CODE__DB'] = 'test'
    os.environ['CODEPACK__CODE__COLLECTION'] = 'codes'
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
        for env in ['CODEPACK__DELIVERY__SOURCE',
                    'CODEPACK__CODE_SNAPSHOT__SOURCE', 'CODEPACK__CODE_SNAPSHOT__PATH',
                    'CODEPACK__CODE__SOURCE', 'CODEPACK__CODE__DB', 'CODEPACK__CODE__COLLECTION']:
            os.environ.pop(env, None)


def test_config_dir():
    path = Config.collect_value(section='?', key='path', config={'path': 'config/test.ini'})
    assert path == 'config/test.ini'
    assert Config.collect_value(section='conn', key='path', config={'path': 'test.ini'}) == 'test.ini'
    with pytest.raises(AssertionError):
        Config.get_config_path('test.ini')
    try:
        os.environ['CODEPACK_CONFIG_DIR'] = 'config'
        assert Config.collect_value(section='conn', key='path', config={'path': 'test.ini'}) == 'test.ini'
        assert Config.get_config_path('test.ini') == os.path.join('config', 'test.ini')
    finally:
        os.environ.pop('CODEPACK_CONFIG_DIR', None)


@patch('codepack.utils.config.config.Config.parse_config')
def test_get_config_without_anything(parse_config):
    config = Config()
    ret = config.get_config('worker')
    assert ret is not None
    arg_list = parse_config.call_args_list
    assert len(arg_list) == 1
    args, kwargs = arg_list[0]
    assert kwargs == {'section': 'worker', 'config_path': config.get_default_config_path()}


@patch('codepack.utils.config.config.Config.parse_config')
def test_get_config_without_anything_but_os_env(parse_config):
    os.environ['CODEPACK_CONFIG_PATH'] = 'config/test.ini'
    try:
        config = Config()
        ret = config.get_config('worker')
        assert ret is not None
        arg_list = parse_config.call_args_list
        assert len(arg_list) == 1
        args, kwargs = arg_list[0]
        assert kwargs == {'section': 'worker', 'config_path': 'config/test.ini'}
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
    os_envs = {'CODEPACK__WORKER__LOGGER': 'dummy-logger',
               'CODEPACK__WORKER__DUMMY': 'dummy_value'}
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
               'CODEPACK__WORKER__LOGGER': 'dummy-logger',
               'CODEPACK__WORKER__DUMMY': 'dummy_value'}
    try:
        for k, v in os_envs.items():
            os.environ[k] = v
        config = Config()
        ret = config.get_config('worker')
        assert ret == {'dummy': 'dummy_value',
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
    os_envs = {'CODEPACK__WORKER__LOGGER': 'dummy-logger',
               'CODEPACK__WORKER__DUMMY': 'dummy_value'}
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
    os_envs = {'CODEPACK__WORKER__LOGGER': 'dummy-logger',
               'CODEPACK__WORKER__DUMMY': 'dummy_value'}
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
        os.environ['CODEPACK__SCHEDULER__DB'] = 'test'
        ret = config.get_storage_config('scheduler')
        ref['db'] = 'test'
        assert ret == ref
        os.environ['CODEPACK__MONGODB__HOST'] = '127.0.0.1'
        os.environ['CODEPACK__MONGODB__USER'] = 'admin'
        ret = config.get_storage_config('scheduler')
        ref['mongodb']['host'] = '127.0.0.1'
        ref['mongodb']['user'] = 'admin'
        assert ret == ref
    finally:
        os.environ.pop('CODEPACK_CONFIG_PATH', None)
        os.environ.pop('CODEPACK__SCHEDULER__DB', None)
        os.environ.pop('CODEPACK__MONGODB__HOST', None)
        os.environ.pop('CODEPACK__MONGODB__USER', None)


def test_get_arbitrary_section():
    try:
        os.environ['CODEPACK_CONFIG_PATH'] = 'config/sample.ini'
        config = Config()
        assert config.get_config('rest_api') == {'my_service': 'https://?:?/?'}
    finally:
        os.environ.pop('CODEPACK_CONFIG_PATH')


def test_get_arbitrary_section_with_os_env():
    try:
        os.environ['CODEPACK__HELLO__WORLD'] = 'hello_world'
        os.environ['CODEPACK__TEST_TEST__TEST_KEY'] = 'test_value'
        config = Config()
        assert config.get_config('hello') == {'world': 'hello_world'}
        assert config.get_config('testtest') == {}
        assert config.get_config('test_test') == {'test_key': 'test_value'}
    finally:
        os.environ.pop('CODEPACK__HELLO__WORLD', None)
        os.environ.pop('CODEPACK__TEST_TEST__TEST_KEY', None)


def test_get_config_without_default():
    try:
        config = Config()
        assert config.get_config('arbitrary') == {}
        os.environ['CODEPACK__SSH__CUSTOM_KEY'] = 'custom_value'
        config1 = Config('config/sample.ini')
        assert config.get_config('arbitrary') == {}
        assert config1.get_config('ssh') == {'ssh_host': '1.2.3.4',
                                             'ssh_password': '?',
                                             'ssh_port': '22',
                                             'ssh_username': '?'}
        config2 = Config()
        assert config.get_config('arbitrary') == {}
        assert config2.get_config('ssh', config_path='config/sample.ini') == {'ssh_host': '1.2.3.4',
                                                                              'ssh_password': '?',
                                                                              'ssh_port': '22',
                                                                              'ssh_username': '?'}
        config3 = Config()
        assert config.get_config('arbitrary') == {}
        assert config3.get_config('ssh') == {'custom_key': 'custom_value', 'ssh_host': 'localhost', 'ssh_port': '22'}
        os.environ['CODEPACK_CONFIG_PATH'] = 'config/sample.ini'
        config4 = Config()
        assert config.get_config('arbitrary') == {}
        assert config4.get_config('ssh') == {'custom_key': 'custom_value',
                                             'ssh_host': '1.2.3.4', 'ssh_password': '?',
                                             'ssh_port': '22', 'ssh_username': '?'}
        os.environ.pop('CODEPACK_CONFIG_PATH', None)
        config5 = Config()
        assert config.get_config('arbitrary') == {}
        assert config5.get_config('ssh', default=False) == {'custom_key': 'custom_value'}
    finally:
        os.environ.pop('CODEPACK_CONFIG_PATH', None)
        os.environ.pop('CODEPACK__SSH__CUSTOM_KEY', None)


def test_get_path_in_logger_section():
    try:
        config = Config()
        os.environ['CODEPACK__LOGGER__TEST_KEY'] = 'test_value'
        default_config_dir = config.get_default_config_dir()
        assert config.get_config('logger') == {'config_path': os.path.join(default_config_dir, 'logging.json'),
                                               'log_dir': 'logs', 'name': 'default-logger', 'test_key': 'test_value'}
        assert config.get_config('logger', default=False) == {'test_key': 'test_value'}
        os.environ['CODEPACK__LOGGER__CONFIG_PATH'] = 'test1.test'
        assert config.get_config('logger') == {'config_path': 'test1.test',
                                               'log_dir': 'logs', 'name': 'default-logger', 'test_key': 'test_value'}
        assert config.get_config('logger', default=False) == {'config_path': 'test1.test', 'test_key': 'test_value'}
        os.environ['CODEPACK__LOGGER__PATH'] = 'test2.test'
        assert config.get_config('logger') == {'config_path': 'test1.test', 'path': 'test2.test',
                                               'log_dir': 'logs', 'name': 'default-logger', 'test_key': 'test_value'}
        assert config.get_config('logger', default=False) == {'config_path': 'test1.test', 'path': 'test2.test',
                                                              'test_key': 'test_value'}
    finally:
        os.environ.pop('CODEPACK__LOGGER__TEST_KEY', None)
        os.environ.pop('CODEPACK__LOGGER__CONFIG_PATH', None)
        os.environ.pop('CODEPACK__LOGGER__PATH', None)
