from typing import Any, Dict, List, Optional
from codepack.code import Code
from codepack.jobs.code_snapshot import CodeSnapshot
from codepack.storages.memory_storage import MemoryStorage
from codepack.storages.file_storage import FileStorage
from codepack.interfaces.random_access_memory import RandomAccessMemory
from codepack.interfaces.file_interface import FileInterface
from codepack.storages.storage_type import StorageType
from tests import add2
import pytest
import os


def test_code_storage_initialization_when_nothing_is_given():
    with pytest.raises(KeyError):
        Code.get_storage()
    assert not Code.storage


def test_code_storage_initialization_when_config_os_env_is_given(os_env_default_config_path):
    storage1 = Code.get_storage()
    assert isinstance(storage1, FileStorage)
    assert storage1.path == 'testdir/codes'
    storage2 = CodeSnapshot.get_storage()
    assert isinstance(storage2, MemoryStorage)
    assert isinstance(storage2.interface, RandomAccessMemory)


def test_initializing_storages_by_os_envs():
    try:
        os_env1 = 'CODEPACK__CODE__TYPE'
        os_env2 = 'CODEPACK__CODE__CONFIG'
        os_env3 = 'CODEPACK__TEST_RAM__'
        os_env4 = 'CODEPACK__CODESNAPSHOT__TYPE'
        os_env5 = 'CODEPACK__CODESNAPSHOT__CONFIG'
        os_env6 = 'CODEPACK__TEST_FILE__PATH'
        os.environ[os_env1] = 'memory'
        os.environ[os_env2] = 'test_ram'
        os.environ[os_env3] = ''
        os.environ[os_env4] = 'file'
        os.environ[os_env5] = 'test_file'
        os.environ[os_env6] = 'testdir/codesnapshot'
        code_storage = Code.get_storage()
        codesnapshot_storage = CodeSnapshot.get_storage()
        assert isinstance(code_storage, MemoryStorage)
        assert isinstance(code_storage.interface, RandomAccessMemory)
        assert isinstance(codesnapshot_storage, FileStorage)
        assert isinstance(codesnapshot_storage.interface, FileInterface)
        assert codesnapshot_storage.path == 'testdir/codesnapshot'
        assert codesnapshot_storage.interface.path == 'testdir/codesnapshot'
    finally:
        for os_env in [os_env1, os_env2, os_env3, os_env4, os_env5, os_env6]:
            os.environ.pop(os_env, None)


def test_registering_storage_alias():
    try:
        os_env1 = 'CODEPACK__CODE__TYPE'
        os_env2 = 'CODEPACK__CODE__CONFIG'
        os_env3 = 'CODEPACK__TEST_FILE__PATH'
        os.environ[os_env1] = 'test_storage'
        os.environ[os_env2] = 'test_file'
        os.environ[os_env3] = 'testdir/codes'
        with pytest.raises(KeyError):
            _ = Code.get_storage()
        assert Code.storage is None
        StorageType.register(name='test_storage', type=FileStorage)
        code_storage = Code.get_storage()
        assert isinstance(code_storage, FileStorage)
        assert isinstance(code_storage.interface, FileInterface)
        assert code_storage.path == 'testdir/codes'
        assert code_storage.interface.path == 'testdir/codes'
    finally:
        for os_env in [os_env1, os_env2, os_env3]:
            os.environ.pop(os_env, None)


def test_registering_custom_storage_extension_as_plugin():
    from codepack.storages.storage import Storage

    class TestStorage(Storage):
        def save(self, id: str, item: Dict[str, Any]) -> bool:
            raise NotImplementedError()

        def load(self, id: str) -> Optional[Dict[str, Any]]:
            return {'test_key': 'test_value'}

        def update(self, id: str, **kwargs: Any) -> bool:
            return True

        def remove(self, id: str) -> bool:
            return True

        def exists(self, id: str) -> bool:
            return True

        def search(self, key: str, value: Any) -> List[str]:
            return ['test']

        def count(self, id: Optional[str]) -> int:
            return -1

        def list_all(self) -> List[str]:
            return ['test']

        def list_like(self, id: str) -> List[str]:
            return ['test']

        def load_many(self, id: Optional[List[str]] = None) -> List[Dict[str, Any]]:
            return []

        def exists_many(self, id: List[str]) -> List[bool]:
            return []

        @classmethod
        def parse_config(cls, config: Dict[str, str]) -> Dict[str, Any]:
            return {}

    StorageType.register(name='test_storage', type=TestStorage)
    try:
        os_env = 'CODEPACK__CODE__TYPE'
        os.environ[os_env] = 'test_storage'
        code_storage = Code.get_storage()
        assert isinstance(code_storage, TestStorage)
        assert code_storage.list_all() == ['test']
        assert code_storage.load(id='test') == {'test_key': 'test_value'}
        code = Code(add2)
        with pytest.raises(NotImplementedError):
            code.save()
    finally:
        os.environ.pop(os_env, None)


def test_unregistering_storage():
    try:
        StorageType.unregister('memory')
        os_env = 'CODEPACK__CODE__TYPE'
        os.environ[os_env] = 'memory'
        with pytest.raises(KeyError):
            _ = Code.get_storage()
        assert Code.storage is None
    finally:
        os.environ.pop(os_env, None)


def test_resetting_storage():
    os_env = 'CODEPACK__CODE__TYPE'
    try:
        StorageType.unregister('memory')
        os.environ[os_env] = 'memory'
        assert set(StorageType.list_types()) == {'file'}
        StorageType.register(name='memory', type=MemoryStorage)
        code_storage = Code.get_storage()
        assert isinstance(code_storage, MemoryStorage)
    finally:
        os.environ.pop(os_env, None)


def test_list_types():
    StorageType.unregister('memory')
    StorageType.register('test', MemoryStorage)
    assert set(StorageType.list_types()) == {'file', 'test'}
