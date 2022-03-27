from unittest.mock import patch
from codepack.config import Default
import os
from codepack.storage import MemoryStorage, MongoStorage
from codepack.service import DeliveryService, CallbackService, SnapshotService
from codepack.scheduler import MongoScheduler
from codepack.employee import Worker, Supervisor, DockerManager, InterpreterManager
from collections.abc import Callable
import inspect
import logging


def test_default_init_with_nothing():
    Default()
    assert len(Default.instances) == 0
    assert Default.config is not None
    assert Default.config.config_path is None
    assert Default.alias is not None
    assert Default.alias.aliases is None


def test_default_init_with_os_env():
    try:
        os.environ['CODEPACK_CONFIG_DIR'] = 'config'
        os.environ['CODEPACK_CONFIG_PATH'] = 'test.ini'
        Default()
        assert len(Default.instances) == 0
        assert Default.config is not None
        assert Default.config.config_path is None
        assert Default.alias is not None
        assert Default.alias.aliases is None
    finally:
        os.environ.pop('CODEPACK_CONFIG_DIR', None)
        os.environ.pop('CODEPACK_CONFIG_PATH', None)


def test_default_init_with_config_path():
    try:
        os.environ['CODEPACK_CONFIG_DIR'] = 'config'
        Default(config_path='config/test.ini')
        assert len(Default.instances) == 0
        assert Default.config is not None
        assert Default.config.config_path == 'config/test.ini'
        assert Default.alias is not None
        assert Default.alias.aliases is None
    finally:
        os.environ.pop('CODEPACK_CONFIG_DIR', None)


def test_default_init_with_config_path_and_alias_path_os_env():
    try:
        os.environ['CODEPACK_ALIAS_PATH'] = 'codepack/config/default/alias.ini'
        Default(config_path='config/test.ini')
        assert len(Default.instances) == 0
        assert Default.config is not None
        assert Default.config.config_path == 'config/test.ini'
        assert Default.alias is not None
        assert Default.alias.aliases is None
    finally:
        os.environ.pop('CODEPACK_ALIAS_PATH', None)


def test_default_init_with_alias_path():
    Default(alias_path='codepack/config/default/alias.ini')
    assert len(Default.instances) == 0
    assert Default.config is not None
    assert Default.config.config_path is None
    assert Default.alias is not None
    assert Default.alias.aliases is not None and isinstance(Default.alias.aliases, dict)


def test_default_get_storage_config():
    try:
        os.environ['CODEPACK_CONFIG_DIR'] = 'config'
        Default(config_path='config/test.ini')
        assert len(Default.instances) == 0
        storage_config = Default._get_storage_config('worker')
        assert storage_config == {'source': 'kafka', 'topic': 'test', 'kafka': {'bootstrap_servers': 'localhost:9092'},
                                  'group_id': 'codepack_worker', 'interval': '1', 'supervisor': 'http://localhost:8000',
                                  'path': 'scripts', 'script': 'run_snapshot.py', 'logger': 'worker-logger'}
    finally:
        os.environ.pop('CODEPACK_CONFIG_DIR', None)


def test_default_get_alias_from_source():
    assert Default.get_alias_from_source(source='memory') == 'memory'
    assert Default.get_alias_from_source(source='memory', prefix='front') == 'front_memory'
    assert Default.get_alias_from_source(source='memory', suffix='back') == 'memory_back'
    assert Default.get_alias_from_source(source='memory', prefix='front', suffix='back') == 'front_memory_back'
    assert Default.get_alias_from_source(source='mongodb') == 'mongo'
    assert Default.get_alias_from_source(source='mongodb', prefix='front') == 'front_mongo'
    assert Default.get_alias_from_source(source='mongodb', suffix='back') == 'mongo_back'
    assert Default.get_alias_from_source(source='mongodb', prefix='front', suffix='back') == 'front_mongo_back'


def test_default_get_class_from_alias():
    Default.config = None
    Default.alias = None
    Default.instances = dict()
    isinstance(Default.get_class_from_alias(alias='memory_storage'), MemoryStorage.__class__)
    try:
        os.environ['CODEPACK_ALIAS_MEMORY_STORAGE'] = 'codepack.storage.file_storage.FileStorage'
        memory_storage = Default.get_class_from_alias(alias='file_storage')
        assert isinstance(memory_storage, MemoryStorage.__class__)
    finally:
        os.environ.pop('CODEPACK_ALIAS_MEMORY_STORAGE', None)
    Default.config = None
    Default.alias = None
    try:
        os.environ['CODEPACK_ALIAS_PATH'] = 'codepack/config/default/alias.ini'
        memory_storage = Default.get_class_from_alias(alias='memory_storage')
        assert isinstance(memory_storage, MemoryStorage.__class__)
    finally:
        os.environ.pop('CODEPACK_ALIAS_PATH', None)
    Default.config = None
    Default.alias = None
    memory_storage = Default.get_class_from_alias(alias='memory_storage', alias_path='codepack/config/default/alias.ini')
    assert isinstance(memory_storage, MemoryStorage.__class__)
    Default.config = None
    Default.alias = None
    try:
        os.environ['CODEPACK_CONFIG_DIR'] = 'config'
        os.environ['CODEPACK_CONFIG_PATH'] = 'config/test.ini'
        memory_storage = Default.get_class_from_alias(alias='memory_storage')
        assert isinstance(memory_storage, MemoryStorage.__class__)
    finally:
        os.environ.pop('CODEPACK_CONFIG_DIR', None)
        os.environ.pop('CODEPACK_CONFIG_PATH', None)


def test_get_default_service():
    service = Default.get_service('delivery', 'delivery_service')
    assert isinstance(service, DeliveryService) and isinstance(service.storage, MemoryStorage)


def test_get_default_scheduler(fake_mongodb):
    try:
        os.environ['CODEPACK_CONN_PATH'] = 'config/test_conn.ini'
        scheduler = Default.get_scheduler()
        assert isinstance(scheduler, MongoScheduler)
        assert scheduler.db == 'codepack'
        assert scheduler.collection == 'scheduler'
        assert scheduler.supervisor is None
    finally:
        os.environ.pop('CODEPACK_CONN_PATH', None)


@patch('docker.DockerClient')
@patch('kafka.KafkaConsumer')
def test_get_default_worker(mock_kafka_consumer, mock_docker_client):
    try:
        os.environ['CODEPACK_CONN_PATH'] = 'config/test_conn.ini'
        worker = Default.get_employee('worker')
        assert isinstance(worker, Worker)
        arg_list = mock_kafka_consumer.call_args_list
        assert len(arg_list) == 1
        args, kwargs = arg_list[0]
        assert kwargs.get('bootstrap_servers', '') == 'localhost:9092'
        assert isinstance(kwargs.get('value_deserializer', ''), Callable)
        assert len(args) == 1 and args[0] == 'test'
        assert worker.consumer.session is mock_kafka_consumer()
        mock_docker_client.assert_called_once_with(base_url='unix://var/run/docker.sock')
        assert worker.docker_manager.docker.session == mock_docker_client()
        assert isinstance(worker.callback_service, CallbackService)
        assert isinstance(worker.logger, logging.Logger)
        assert worker.logger.name == 'worker-logger'
    finally:
        os.environ.pop('CODEPACK_CONN_PATH', None)


@patch('kafka.KafkaProducer')
def test_get_default_supervisor(mock_kafka_producer):
    try:
        os.environ['CODEPACK_CONN_PATH'] = 'config/test_conn.ini'
        os.environ['CODEPACK_CODESNAPSHOT_SOURCE'] = 'mongodb'
        os.environ['CODEPACK_CODESNAPSHOT_DB'] = 'test_db'
        os.environ['CODEPACK_CODESNAPSHOT_COLLECTION'] = 'test_collection'
        supervisor = Default.get_employee('supervisor')
        assert isinstance(supervisor, Supervisor)
        arg_list = mock_kafka_producer.call_args_list
        assert len(arg_list) == 1
        args, kwargs = arg_list[0]
        assert kwargs.get('bootstrap_servers', '') == 'localhost:9092'
        assert isinstance(kwargs.get('value_serializer', ''), Callable)
        assert len(args) == 0
        assert supervisor.producer.session is mock_kafka_producer()
        assert isinstance(supervisor.snapshot_service, SnapshotService)
        assert isinstance(supervisor.snapshot_service.storage, MongoStorage)
    finally:
        os.environ.pop('CODEPACK_CONN_PATH', None)
        os.environ.pop('CODEPACK_CODESNAPSHOT_SOURCE', None)
        os.environ.pop('CODEPACK_CODESNAPSHOT_DB', None)
        os.environ.pop('CODEPACK_CODESNAPSHOT_COLLECTION', None)


@patch('docker.DockerClient')
def test_get_default_docker_manager(mock_client):
    try:
        os.environ['CODEPACK_CONN_PATH'] = 'config/test_conn.ini'
        docker_manager = Default.get_docker_manager()
        mock_client.assert_called_once_with(base_url='unix://var/run/docker.sock')
        assert isinstance(docker_manager, DockerManager)
        default_dir = os.path.dirname(os.path.abspath(inspect.getfile(DockerManager)))
        assert docker_manager.path == os.path.join(default_dir, 'scripts')
        assert docker_manager.run_opt == {'dns': ['8.8.8.8']}
    finally:
        os.environ.pop('CODEPACK_CONN_PATH', None)


def test_get_default_interpreter_manager():
    try:
        os.environ['CODEPACK_CONN_PATH'] = 'config/test_conn.ini'
        interpreter_manager = Default.get_interpreter_manager()
        assert isinstance(interpreter_manager, InterpreterManager)
        assert interpreter_manager.path == '${HOME}/anaconda3/envs'
        assert interpreter_manager.run_opt == {}
    finally:
        os.environ.pop('CODEPACK_CONN_PATH', None)


def test_get_default_logger():
    logger = Default.get_logger()
    assert isinstance(logger, logging.Logger)
    assert logger.name == 'default-logger'
    assert len(logger.handlers) == 1
    assert logger.handlers[0].get_name() == 'console'
    logger2 = Default.get_logger('worker-logger')
    assert isinstance(logger2, logging.Logger)
    assert logger2.name == 'worker-logger'
    assert len(logger2.handlers) == 2
