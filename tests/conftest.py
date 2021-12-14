import pytest
from codepack.interface import MongoDB
import os
import mongomock
from codepack.service import DefaultServicePack


@pytest.fixture(scope='function', autouse=False)
def default_os_env():
    os.environ['CODEPACK_CONFIG_PATH'] = 'config/test.ini'
    yield
    os.environ.pop('CODEPACK_CONFIG_PATH', None)


@pytest.fixture(scope='session', autouse=True)
def fake_mongodb():
    mongodb = MongoDB({'host': 'unknown', 'port': '1234'})
    mongodb.client = mongomock.MongoClient()
    yield mongodb
    mongodb.close()


@pytest.fixture(scope='function', autouse=True)
def init_default_service_pack():
    DefaultServicePack.init()


@pytest.fixture(scope='function', autouse=False)
def default_services():
    DefaultServicePack.init()
    yield DefaultServicePack
    DefaultServicePack.init()
