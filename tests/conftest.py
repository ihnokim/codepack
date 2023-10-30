import pytest
from codepack.code import Code
from codepack.jobs.code_snapshot import CodeSnapshot
from codepack.jobs.async_code_snapshot import AsyncCodeSnapshot
from codepack.jobs.codepack_snapshot import CodePackSnapshot
from codepack.jobs.async_codepack_snapshot import AsyncCodePackSnapshot
from codepack.jobs.job import Job
from codepack.jobs.async_job import AsyncJob
from codepack.jobs.result_cache import ResultCache
from codepack.jobs.async_result_cache import AsyncResultCache
from codepack.storages.memory_storage import MemoryStorage
from codepack.storages.async_memory_storage import AsyncMemoryStorage
from codepack.jobs.async_worker import AsyncWorker
from codepack.jobs.worker import Worker
from codepack.messengers.receiver import Receiver
from codepack.messengers.async_receiver import AsyncReceiver
from codepack.messengers.sender import Sender
from codepack.messengers.async_sender import AsyncSender
from codepack.interfaces.memory_message_queue import MemoryMessageQueue
from codepack.interfaces.async_memory_message_queue import AsyncMemoryMessageQueue
from codepack.interfaces.file_message_queue import FileMessageQueue
from codepack.interfaces.async_file_message_queue import AsyncFileMessageQueue
from codepack.jobs.lock import Lock
from codepack.jobs.async_lock import AsyncLock
from codepack.storages.storage_type import StorageType
from codepack.jobs.supervisor import Supervisor
from codepack.jobs.async_supervisor import AsyncSupervisor
import os
from shutil import rmtree
from tests import test_config_path, default_config_path
from copy import deepcopy


@pytest.fixture(scope='function', autouse=False)
def init_storages():
    code_ms = MemoryStorage()
    code_snapshot_ms = MemoryStorage()
    async_code_snapshot_ms = AsyncMemoryStorage()
    codepack_snapshot_ms = MemoryStorage()
    async_codepack_snapshot_ms = AsyncMemoryStorage()
    job_ms = MemoryStorage()
    async_job_ms = AsyncMemoryStorage()
    result_cache_ms = MemoryStorage()
    async_result_cache_ms = AsyncMemoryStorage()
    lock_ms = MemoryStorage()
    async_lock_ms = AsyncMemoryStorage()
    Code.set_storage(storage=code_ms)
    CodeSnapshot.set_storage(storage=code_snapshot_ms)
    AsyncCodeSnapshot.set_storage(storage=async_code_snapshot_ms)
    CodePackSnapshot.set_storage(storage=codepack_snapshot_ms)
    AsyncCodePackSnapshot.set_storage(storage=async_codepack_snapshot_ms)
    Job.set_storage(storage=job_ms)
    AsyncJob.set_storage(storage=async_job_ms)
    ResultCache.set_storage(storage=result_cache_ms)
    AsyncResultCache.set_storage(storage=async_result_cache_ms)
    Lock.set_storage(storage=lock_ms)
    AsyncLock.set_storage(storage=async_lock_ms)
    MemoryMessageQueue().clear()
    AsyncMemoryMessageQueue().clear()
    FileMessageQueue('testdir').clear()
    AsyncFileMessageQueue('testdir').clear()
    yield


@pytest.fixture(scope='function', autouse=True)
def clear_storages():
    storage_type_backup = deepcopy(StorageType.types)
    yield
    Code.storage = None
    CodeSnapshot.storage = None
    AsyncCodeSnapshot.storage = None
    CodePackSnapshot.storage = None
    AsyncCodePackSnapshot.storage = None
    Job.storage = None
    AsyncJob.storage = None
    ResultCache.storage = None
    AsyncResultCache.storage = None
    Lock.storage = None
    AsyncLock.storage = None
    MemoryMessageQueue().clear()
    AsyncMemoryMessageQueue().clear()
    FileMessageQueue('testdir').clear()
    AsyncFileMessageQueue('testdir').clear()
    StorageType.types = storage_type_backup


@pytest.fixture(scope='function', autouse=False)
def testdir():
    testdir_path = 'testdir'
    if os.path.exists(testdir_path):
        rmtree(testdir_path)
    yield testdir_path


@pytest.fixture(scope='function', autouse=True)
def remove_testdir():
    testdir_path = 'testdir'
    yield
    if os.path.exists(testdir_path):
        rmtree(testdir_path)


@pytest.fixture(scope='function', autouse=False)
def memory_job_managers():
    worker_mr = Receiver(topics=['test-jobs'], group='worker')
    supervisor_mr = Receiver(topics=['test-jobs'], group='supervisor')
    ms = Sender()
    _supervisor = Supervisor(topic='test-jobs',
                             receiver=supervisor_mr,
                             sender=ms)
    _worker = Worker(topic='test-jobs',
                     receiver=worker_mr,
                     sender=ms)
    yield _supervisor, _worker
    supervisor_mr.close()
    worker_mr.close()
    _supervisor.stop()
    _worker.stop()


@pytest.fixture(scope='function', autouse=False)
def file_job_managers():
    worker_mr = Receiver(topics=['test-jobs'], group='worker', message_queue=FileMessageQueue(path='testdir'))
    supervisor_mr = Receiver(topics=['test-jobs'], group='supervisor', message_queue=FileMessageQueue(path='testdir'))
    ms = Sender(message_queue=FileMessageQueue(path='testdir'))
    _supervisor = Supervisor(topic='test-jobs',
                             receiver=supervisor_mr,
                             sender=ms)
    _worker = Worker(topic='test-jobs',
                     receiver=worker_mr,
                     sender=ms)
    yield _supervisor, _worker
    supervisor_mr.close()
    worker_mr.close()
    _supervisor.stop()
    _worker.stop()


@pytest.fixture(scope='function', autouse=False)
def async_memory_job_managers():
    worker_mr = AsyncReceiver(topics=['test-jobs'], group='worker')
    supervisor_mr = AsyncReceiver(topics=['test-jobs'], group='supervisor')
    ms = AsyncSender()
    _supervisor = AsyncSupervisor(topic='test-jobs',
                                  receiver=supervisor_mr,
                                  sender=ms)
    _worker = AsyncWorker(topic='test-jobs',
                          receiver=worker_mr,
                          sender=ms)
    yield _supervisor, _worker
    supervisor_mr.stop_event.set()
    worker_mr.stop_event.set()


@pytest.fixture(scope='function', autouse=False)
def async_file_job_managers():
    worker_mr = AsyncReceiver(topics=['test-jobs'],
                              group='worker',
                              message_queue=AsyncFileMessageQueue(path='testdir'))
    supervisor_mr = AsyncReceiver(topics=['test-jobs'],
                                  group='supervisor',
                                  message_queue=AsyncFileMessageQueue(path='testdir'))
    ms = AsyncSender(message_queue=AsyncFileMessageQueue(path='testdir'))
    _supervisor = AsyncSupervisor(topic='test-jobs',
                                  receiver=supervisor_mr,
                                  sender=ms)
    _worker = AsyncWorker(topic='test-jobs',
                          receiver=worker_mr,
                          sender=ms)
    yield _supervisor, _worker
    supervisor_mr.stop_event.set()
    worker_mr.stop_event.set()


@pytest.fixture(scope='function', autouse=False)
def os_env_test_config_path():
    os.environ['CODEPACK_CONFIG_PATH'] = test_config_path
    yield os.environ['CODEPACK_CONFIG_PATH']
    os.environ.pop('CODEPACK_CONFIG_PATH', None)


@pytest.fixture(scope='function', autouse=False)
def os_env_default_config_path():
    os.environ['CODEPACK_CONFIG_PATH'] = default_config_path
    yield os.environ['CODEPACK_CONFIG_PATH']
    os.environ.pop('CODEPACK_CONFIG_PATH', None)
