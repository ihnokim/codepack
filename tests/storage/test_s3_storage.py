from codepack import Delivery, Config
from codepack.storage import S3Storage
from unittest.mock import patch


@patch('boto3.client')
def test_s3_storage(mock_client, dummy_deliveries):
    config = Config()
    s3_config = config.get_config('s3')
    ss = S3Storage(s3=s3_config, item_type=Delivery, key='id')
    mock_client.assert_called_once()
    assert ss.s3.session is mock_client.return_value
    ss.close()
    assert not ss.s3


@patch('boto3.client')
def test_s3_storage_list_all(mock_client):
    config = Config()
    s3_config = config.get_config('s3')
    ss = S3Storage(s3=s3_config, item_type=Delivery, key='id', bucket='test_bucket', path='test_path')
    mock_client.return_value.get_paginator.return_value.paginate.return_value\
        = [{'Contents': [{'Key': 'test_path/x1.json'}, {'Key': 'test_path/x2.json'}]}]
    k = sorted(ss.list_all())
    mock_client.return_value.get_paginator.assert_called_once_with('list_objects')
    mock_client.return_value.get_paginator.return_value.paginate.assert_called_once_with(Bucket='test_bucket',
                                                                                         Prefix='test_path/')
    assert k == ['x1', 'x2']
    ss.close()
    assert not ss.s3
