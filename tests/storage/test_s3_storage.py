from codepack import Delivery, Config
from codepack.storage import S3Storage
from unittest.mock import patch


@patch('boto3.client')
def test_s3_storage(mock_client, dummy_deliveries):
    config = Config()
    s3_config = config.get_config('s3')
    ss = S3Storage(s3=s3_config, item_type=Delivery, key='id')
    assert ss.s3.session is mock_client()
    ss.close()
    assert not ss.s3
