import pytest
import os
from tests import add2
from collections import OrderedDict
from codepack.code import Code
from codepack.storages.file_storage import FileStorage
from codepack.storages.memory_storage import MemoryStorage
from codepack.jobs.worker import Worker
from codepack.jobs.supervisor import Supervisor
from codepack.messengers.sender import Sender


def test_code_storage_initialization_without_anything():
    code = Code(add2)
    with pytest.raises(KeyError):
        code.save()


def test_code_storage_initialization_with_default_config_file(os_env_default_config_path):
    code = Code(add2)
    try:
        assert code.save()
        storage = code.get_storage()
        assert isinstance(storage, FileStorage)
        assert storage.list_all() == [code.get_id()]
    finally:
        storage.remove(code.get_id())


def test_code_storage_initialization_with_os_env():
    os_envs = {'CODEPACK__CODE__TYPE': 'memory',
               'CODEPACK__CODE__CONFIG': 'test',
               'CODEPACK__TEST__': ''}
    try:
        for os_env, value in os_envs.items():
            os.environ[os_env] = value
        code = Code(add2)
        assert code.save()
        storage = code.get_storage()
        assert isinstance(storage, MemoryStorage)
        assert storage.list_all() == [code.get_id()]
    finally:
        for os_env in os_envs.keys():
            os.environ.pop(os_env, None)


def test_memory_storage_initialization_with_empty_config():
    with pytest.raises(KeyError):
        _ = MemoryStorage.from_config('memory')


def test_memory_storage_initialization_with_os_env():
    os_envs = {'CODEPACK__MEMORY1__KEEP_ORDER': 'true',
               'CODEPACK__MEMORY2__KEEP_ORDER': 'false'}
    try:
        for os_env, value in os_envs.items():
            os.environ[os_env] = value
        storage1 = MemoryStorage.from_config('memory1')
        storage2 = MemoryStorage.from_config('memory2')
        assert isinstance(storage1.memory, OrderedDict)
        assert not isinstance(storage2.memory, OrderedDict)
    finally:
        for os_env in os_envs.keys():
            os.environ.pop(os_env, None)


def test_worker_initialization_without_nothing():
    with pytest.raises(KeyError):
        Worker.from_config(section='worker')


def test_worker_initialization_with_default_config_file(os_env_default_config_path):
    worker = Worker.from_config(section='worker')
    assert worker.background is True
    assert worker.topic == 'codepack'
    assert worker.receiver.group == 'worker'


def test_worker_initialization_with_os_env():
    os_envs = {'CODEPACK__TEST_WORKER__TOPIC': 'test_topic',
               'CODEPACK__TEST_WORKER__SENDER': 'test_sender',
               'CODEPACK__TEST_WORKER__RECEIVER': 'test_receiver',
               'CODEPACK__TEST_WORKER__BACKGROUND': 'false',
               'CODEPACK__TEST_SENDER__TYPE': 'memory',
               'CODEPACK__TEST_RECEIVER__TYPE': 'memory',
               'CODEPACK__TEST_RECEIVER__CONFIG': 'test_receiver_config',
               'CODEPACK__TEST_RECEIVER_CONFIG__TOPICS': 'test1,test2',
               'CODEPACK__TEST_RECEIVER_CONFIG__GROUP': 'test_group'}
    try:
        for os_env, value in os_envs.items():
            os.environ[os_env] = value
        worker = Worker.from_config(section='test_worker')
        assert worker.background is False
        assert worker.topic == 'test_topic'
        assert worker.receiver.group == 'test_group'
        assert worker.receiver.topics == ['test1', 'test2']
    finally:
        for os_env in os_envs.keys():
            os.environ.pop(os_env, None)


def test_supervisor_initialization_with_default_config_file(os_env_default_config_path):
    supervisor = Supervisor.from_config(section='supervisor')
    assert supervisor.background is False
    assert supervisor.topic == 'codepack'
    assert supervisor.receiver.group == 'supervisor'


def test_supervisor_initialization_with_os_env():
    os_envs = {'CODEPACK__TEST_SUPERVISOR__TOPIC': 'test_topic',
               'CODEPACK__TEST_SUPERVISOR__SENDER': 'test_sender',
               'CODEPACK__TEST_SUPERVISOR__RECEIVER': 'test_receiver',
               'CODEPACK__TEST_SUPERVISOR__BACKGROUND': 'true',
               'CODEPACK__TEST_SENDER__TYPE': 'memory',
               'CODEPACK__TEST_SENDER__CONFIG': 'ms',
               'CODEPACK__MS__': '',
               'CODEPACK__TEST_RECEIVER__TYPE': 'memory',
               'CODEPACK__TEST_RECEIVER__CONFIG': 'test_receiver_config',
               'CODEPACK__TEST_RECEIVER_CONFIG__TOPICS': 'test1, test2, test3',
               'CODEPACK__TEST_RECEIVER_CONFIG__GROUP': 'test_group'}
    try:
        for os_env, value in os_envs.items():
            os.environ[os_env] = value
        supervisor = Supervisor.from_config(section='test_supervisor')
        assert supervisor.background is True
        assert supervisor.topic == 'test_topic'
        assert supervisor.receiver.group == 'test_group'
        assert supervisor.receiver.topics == ['test1', 'test2', 'test3']
    finally:
        for os_env in os_envs.keys():
            os.environ.pop(os_env, None)


def test_memory_sender_initialization_without_nothing():
    ms = Sender.from_config()
    assert isinstance(ms, Sender)


def test_memory_sender_initialization_with_os_env():
    ms = Sender.from_config()
    assert isinstance(ms, Sender)
    os_envs = {'CODEPACK__TEST_MS__': ''}
    try:
        for os_env, value in os_envs.items():
            os.environ[os_env] = value
        ms = Sender.from_config('test_ms')
        assert isinstance(ms, Sender)
    finally:
        for os_env in os_envs.keys():
            os.environ.pop(os_env, None)
