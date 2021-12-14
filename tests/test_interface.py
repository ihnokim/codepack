from codepack.utils.config import get_config
from codepack.interface import MongoDB


def test_singleton_interface_mongodb():
    mongodb_config = get_config('config/test_conn.ini', section='mongodb')
    mongodb1 = MongoDB(mongodb_config)
    mongodb2 = MongoDB(mongodb_config)
    assert mongodb1 != mongodb2
    mongodb1.close()
    mongodb2.close()
