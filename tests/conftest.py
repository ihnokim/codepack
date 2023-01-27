from codepack import Default, Delivery
from codepack.interfaces import MongoDB
from codepack.interfaces import MemoryMessageQueue
from apps.apiserver.main import app
from fastapi.testclient import TestClient
import pytest
import os
import mongomock


from shutil import rmtree
from glob import glob


def mkdir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def rmdir(directory):
    if os.path.exists(directory):
        rmtree(directory)


def empty_dir(directory):
    if directory[-1] != '/' and directory[-1] != '\\':
        tmp = '/*'
    else:
        tmp = '*'
    files = glob(directory + tmp)
    for file in files:
        os.remove(file)


@pytest.fixture(scope='function', autouse=False)
def default_os_env():
    os.environ['CODEPACK_CONFIG_DIR'] = 'config'
    os.environ['CODEPACK_CONFIG_PATH'] = 'test.ini'
    Default.get_service('delivery', 'delivery_service').storage.init()
    Default.get_service('code_snapshot', 'snapshot_service').storage.init()
    Default.get_service('code', 'storage_service').storage.init()
    Default.get_service('codepack', 'storage_service').storage.init()
    yield
    os.environ.pop('CODEPACK_CONFIG_DIR', None)
    os.environ.pop('CODEPACK_CONFIG_PATH', None)


@pytest.fixture(scope='function', autouse=False)
def fake_mongodb():
    mongodb = MongoDB({'host': 'unknown', 'port': '0'})
    mongodb.session = mongomock.MongoClient()
    yield mongodb
    mongodb.close()


@pytest.fixture(scope='session', autouse=True)
def testdir():
    rootdir = 'testdir/'
    mkdir(rootdir)
    mkdir(rootdir + 'delivery_service/')
    mkdir(rootdir + 'state_manager/')
    mkdir(rootdir + 'storage_service/')
    mkdir(rootdir + 'snapshot_service/')
    mkdir(rootdir + 'docker_test/')
    mkdir(rootdir + 'scheduler/')
    yield
    rmdir(rootdir)


@pytest.fixture(scope='function', autouse=False)
def testdir_delivery_service():
    directory = 'testdir/delivery_service/'
    empty_dir(directory)
    yield directory
    empty_dir(directory)


@pytest.fixture(scope='function', autouse=False)
def testdir_storage_service():
    directory = 'testdir/storage_service/'
    empty_dir(directory)
    yield directory
    empty_dir(directory)


@pytest.fixture(scope='function', autouse=False)
def testdir_snapshot_service():
    directory = 'testdir/snapshot_service/'
    empty_dir(directory)
    yield directory
    empty_dir(directory)


@pytest.fixture(scope='function', autouse=False)
def testdir_docker_manager():
    directory = 'testdir/docker_test/'
    empty_dir(directory)
    yield directory
    empty_dir(directory)


@pytest.fixture(scope='function', autouse=False)
def testdir_file_storage():
    directory = 'testdir/file_storage/'
    empty_dir(directory)
    yield directory
    empty_dir(directory)


@pytest.fixture(scope='function', autouse=True)
def init_default():
    Default.alias = None
    Default.config = None
    Default.instances = dict()
    MemoryMessageQueue.remove_all_instances()


@pytest.fixture(scope='function', autouse=False)
def dummy_deliveries():
    obj1 = Delivery(name='obj1', serial_number='123', item='x')
    obj2 = Delivery(name='obj2', serial_number='456', item='y')
    obj3 = Delivery(name='obj3', serial_number='789', item='y')
    yield [obj1, obj2, obj3]


@pytest.fixture(scope='function', autouse=False)
def dummy_deliveries_for_text_key_search():
    obj1 = Delivery(name='apple_orange', serial_number='123', item='x')
    obj2 = Delivery(name='orange_banana', serial_number='456', item='y')
    obj3 = Delivery(name='banana_apple', serial_number='789', item='z')
    yield [obj1, obj2, obj3]


@pytest.fixture
def test_client():
    with TestClient(app) as client:
        yield client


@pytest.fixture
def test_worker():
    worker = Default.get_employee('worker')
    worker.start()
    yield worker
    worker.stop()
