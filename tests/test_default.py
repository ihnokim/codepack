from codepack.config import Default
import os
from codepack.storage import MemoryStorage


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
