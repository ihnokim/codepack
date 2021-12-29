import pytest
from codepack.interface import MongoDB
import os
import mongomock
from codepack.service import DefaultServicePack
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
    os.environ['CODEPACK_CONFIG_PATH'] = 'config/test.ini'
    yield
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
    yield
    rmdir(rootdir)


@pytest.fixture(scope='function', autouse=False)
def testdir_delivery_service():
    directory = 'testdir/delivery_service/'
    empty_dir(directory)
    yield directory
    empty_dir(directory)


@pytest.fixture(scope='function', autouse=False)
def testdir_state_manager():
    directory = 'testdir/state_manager/'
    empty_dir(directory)
    yield directory
    empty_dir(directory)


@pytest.fixture(scope='function', autouse=False)
def testdir_storage_service():
    directory = 'testdir/storage_service/'
    empty_dir(directory)
    yield directory
    empty_dir(directory)


@pytest.fixture(scope='function', autouse=True)
def init_default_service_pack():
    DefaultServicePack.init()


@pytest.fixture(scope='function', autouse=False)
def default_services():
    DefaultServicePack.init()
    yield DefaultServicePack
    DefaultServicePack.init()
