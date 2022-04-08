import unittest.mock
from unittest.mock import patch
from codepack.interfaces import S3
from botocore.config import Config


@patch('boto3.client')
def test_s3_init(mock_client):
    s3_config = {'service_name': 's3', 'region_name': 'test_region', 'endpoint_url': 'test_url',
                 'aws_access_key_id': 'test_access_key_id', 'aws_secret_access_key': 'test_secret_access_key'}
    s = S3(config=s3_config)
    arg_list = mock_client.call_args_list
    assert len(arg_list) == 1
    args, kwargs = arg_list[0]
    assert kwargs.get('service_name', '') == 's3'
    assert kwargs.get('region_name', '') == 'test_region'
    assert kwargs.get('endpoint_url', '') == 'test_url'
    assert kwargs.get('aws_access_key_id', '') == 'test_access_key_id'
    assert kwargs.get('aws_secret_access_key', '') == 'test_secret_access_key'
    assert isinstance(kwargs.get('config', None), Config)
    assert kwargs.get('config').retries == {'max_attempts': 3}
    assert 'config' in s.config and s.config['config'] == kwargs.get('config')
    assert s.session is mock_client()
    s.close()
    mock_client().close.assert_not_called()
    assert s.closed()


@patch('boto3.client')
def test_s3_list_objects(mock_client):
    s3_config = {'service_name': 's3', 'region_name': 'test_region', 'endpoint_url': 'test_url',
                 'aws_access_key_id': 'test_access_key_id', 'aws_secret_access_key': 'test_secret_access_key'}
    s = S3(config=s3_config)
    ret = s.list_objects(bucket='test_bucket', prefix='test_prefix')
    mock_client().get_paginator.assert_called_once_with('list_objects')
    mock_client().get_paginator.return_value.paginate.assert_called_once()
    assert len(ret) == 0
    mock_client().get_paginator.return_value.paginate.return_value = [{'Contents': ['test1']}, {'Dummy': ['test2']},
                                                                      {'Contents': ['test3', 'test4']}]
    ret2 = s.list_objects(bucket='test_bucket', prefix='test_prefix')
    assert ret2 == ['test1', 'test3', 'test4']


@patch('boto3.client')
def test_s3_download(mock_client):
    s3_config = {'service_name': 's3', 'region_name': 'test_region', 'endpoint_url': 'test_url',
                 'aws_access_key_id': 'test_access_key_id', 'aws_secret_access_key': 'test_secret_access_key'}
    s = S3(config=s3_config)
    mc = mock_client()
    response_body = unittest.mock.MagicMock()
    mc.get_object.return_value.get.return_value = response_body
    ret = s.download(bucket='test_bucket', key='test_dir/test_file', streaming=True)
    mc.get_object.assert_called_once_with(Bucket='test_bucket', Key='test_dir/test_file')
    response_body.read.assert_not_called()
    assert ret is not None
    ret = s.download(bucket='test_bucket', key='test_dir/test_file', streaming=False)
    assert mc.get_object.call_count == 2
    response_body.read.assert_called_once()
    assert ret is not None


@patch('boto3.client')
def test_s3_exist(mock_client):
    s3_config = {'service_name': 's3', 'region_name': 'test_region', 'endpoint_url': 'test_url',
                 'aws_access_key_id': 'test_access_key_id', 'aws_secret_access_key': 'test_secret_access_key'}
    s = S3(config=s3_config)
    ret = s.exist(bucket='test_bucket', key='test_dir/test_file')
    mc = mock_client()
    mc.head_object.assert_called_once_with(Bucket='test_bucket', Key='test_dir/test_file')
    assert ret is not False


@patch('boto3.client')
def test_s3_delete(mock_client):
    s3_config = {'service_name': 's3', 'region_name': 'test_region', 'endpoint_url': 'test_url',
                 'aws_access_key_id': 'test_access_key_id', 'aws_secret_access_key': 'test_secret_access_key'}
    s = S3(config=s3_config)
    s.delete(bucket='test_bucket', key='test_dir/test_file')
    mc = mock_client()
    mc.delete_object.assert_called_once_with(Bucket='test_bucket', Key='test_dir/test_file')


@patch('boto3.client')
def test_s3_upload(mock_client):
    s3_config = {'service_name': 's3', 'region_name': 'test_region', 'endpoint_url': 'test_url',
                 'aws_access_key_id': 'test_access_key_id', 'aws_secret_access_key': 'test_secret_access_key'}
    s = S3(config=s3_config)
    s.upload(bucket='test_bucket', key='test_dir/test_file', data=b'12345678')
    mc = mock_client()
    mc.put_object.assert_called_once_with(Bucket='test_bucket', Key='test_dir/test_file', Body=b'12345678')
