import pytest
from codepack.interface import MongoDB
import os
import mongomock
from codepack.service import DefaultService
from codepack import Code, CodePack
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
    DefaultService.get_default_delivery_service().init()
    DefaultService.get_default_code_snapshot_service().init()
    DefaultService.get_default_code_storage_service(obj=Code).init()
    DefaultService.get_default_codepack_storage_service(obj=CodePack).init()
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
    mkdir(rootdir + 'snapshot_service/')
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


@pytest.fixture(scope='function', autouse=True)
def init_default_service():
    DefaultService.init()


@pytest.fixture(scope='function', autouse=False)
def default_services():
    DefaultService.init()
    yield DefaultService
    DefaultService.init()
