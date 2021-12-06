import pytest
from codepack.utils.config import get_config
from codepack.interface import MongoDB
import os


@pytest.fixture(scope='session', autouse=True)
def mongodb():
    mongodb_config = get_config('config/test_conn.ini', section='mongodb')
    mongodb = MongoDB(mongodb_config)
    yield mongodb
    mongodb.close()


@pytest.fixture(scope='function', autouse=False)
def default_os_env():
    os.environ['CODEPACK_CONFIG_PATH'] = 'config/test.ini'
    yield
    os.environ.pop('CODEPACK_CONFIG_PATH', None)
