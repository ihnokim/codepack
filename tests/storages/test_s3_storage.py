from codepack import Delivery, Config
from codepack.storages import S3Storage
from codepack.interfaces import S3
from unittest.mock import patch, MagicMock
import pytest
import json
from functools import partial


def fake_get_object(dummy_deliveries, Key, *args, **kwargs):
    tmp = {x.get_path(key=x.id, path='test_path', posix=True): {'Body': x.to_json()} for x in dummy_deliveries}
    ret = tmp.get(Key, dict())
    if ret:
        body = ret['Body']
        ret['Body'] = MagicMock()
        ret['Body'].read.return_value = body
    return ret


@patch('boto3.client')
def test_s3_storage_init_with_s3(mock_client):
    config = Config()
    s3_config = config.get_config('s3')
    s3 = S3(s3_config)
    ss = S3Storage(s3=s3, item_type=Delivery, key='id')
    arg_list = mock_client.call_args_list
    assert len(arg_list) == 1
    assert not ss.new_connection
    args, kwargs = arg_list[0]
    assert kwargs.get('service_name', '') == 's3'
    assert kwargs.get('region_name', '') == ''
    assert kwargs.get('endpoint_url', '') == ''
    assert kwargs.get('aws_access_key_id', '') == 'codepack'
    assert kwargs.get('aws_secret_access_key', '') == 'codepack'
    assert 'config' in ss.s3.config and ss.s3.config['config'] == kwargs.get('config')
    assert kwargs.get('config').retries == {'max_attempts': 3}
    assert ss.s3.session is mock_client.return_value
    close_function = MagicMock()
    ss.s3.close = close_function
    ss.close()
    close_function.assert_not_called()
    assert not ss.s3


@patch('boto3.client')
def test_s3_storage_init_with_dict(mock_client):
    config = Config()
    s3_config = config.get_config('s3')
    ss = S3Storage(s3=s3_config, item_type=Delivery, key='id')
    arg_list = mock_client.call_args_list
    assert len(arg_list) == 1
    assert ss.new_connection
    args, kwargs = arg_list[0]
    assert kwargs.get('service_name', '') == 's3'
    assert kwargs.get('region_name', '') == ''
    assert kwargs.get('endpoint_url', '') == ''
    assert kwargs.get('aws_access_key_id', '') == 'codepack'
    assert kwargs.get('aws_secret_access_key', '') == 'codepack'
    assert 'config' in ss.s3.config and ss.s3.config['config'] == kwargs.get('config')
    assert kwargs.get('config').retries == {'max_attempts': 3}
    assert ss.s3.session is mock_client.return_value
    close_function = MagicMock()
    ss.s3.close = close_function
    ss.close()
    close_function.assert_called_once()
    assert not ss.s3


@patch('boto3.client')
def test_s3_storage_exist(mock_client):
    config = Config()
    s3_config = config.get_config('s3')
    ss = S3Storage(s3=s3_config, item_type=Delivery, key='id', bucket='test_bucket', path='test_path')
    ret = ss.exist(key='test')
    mock_client.return_value.head_object.assert_called_once_with(Key='test_path/test.json', Bucket='test_bucket')
    assert ret is True
    ret = ss.exist(key=['test1', 'test2'])
    assert mock_client.return_value.head_object.call_count == 3
    assert ret == [True, True]
    ret = ss.exist(key=['test1', 'test2'], summary='or')
    assert mock_client.return_value.head_object.call_count == 4
    assert ret is True
    ret = ss.exist(key=['test1', 'test2'], summary='and')
    assert mock_client.return_value.head_object.call_count == 6
    assert ret is True


@patch('boto3.client')
def test_s3_storage_remove(mock_client):
    config = Config()
    s3_config = config.get_config('s3')
    ss = S3Storage(s3=s3_config, item_type=Delivery, key='id', bucket='test_bucket', path='test_path')
    ss.remove(key='test')
    mock_client.return_value.delete_object.assert_called_once_with(Bucket='test_bucket', Key='test_path/test.json')
    ss.remove(key=['test1', 'test2', 'test3'])
    assert mock_client.return_value.delete_object.call_count == 4


@patch('boto3.client')
def test_s3_storage_search(mock_client, dummy_deliveries):
    config = Config()
    s3_config = config.get_config('s3')
    ss = S3Storage(s3=s3_config, item_type=Delivery, key='id', bucket='test_bucket', path='test_path')
    ret = ss.search(key='item', value='y')
    mock_client.return_value.get_paginator.assert_called_once_with('list_objects')
    mock_client.return_value.get_paginator.return_value.paginate.assert_called_once_with(Bucket='test_bucket',
                                                                                         Prefix='test_path/')
    assert ret == list()
    mock_client.return_value.get_paginator.return_value.paginate.return_value \
        = [{'Contents': [{'Key': 'test_path/obj1.json'}, {'Key': 'test_path/obj2.json'},
                         {'Key': 'test_path/obj3.json'}]}]
    mock_client.return_value.exceptions.NoSuchKey = Exception
    mock_client.return_value.get_object.side_effect = partial(fake_get_object, dummy_deliveries)
    ret = ss.search(key='item', value=json.dumps('y'))
    assert len(ret) == 2
    assert isinstance(ret[0], Delivery)
    assert sorted([obj.id for obj in ret]) == ['obj2', 'obj3']
    arg_list = mock_client.return_value.get_object.call_args_list
    assert len(arg_list) == 3
    args, kwargs = arg_list[0]
    assert kwargs.get('Bucket', '') == 'test_bucket'
    assert kwargs.get('Key', '') == 'test_path/obj1.json'
    args, kwargs = arg_list[1]
    assert kwargs.get('Bucket', '') == 'test_bucket'
    assert kwargs.get('Key', '') == 'test_path/obj2.json'
    args, kwargs = arg_list[2]
    assert kwargs.get('Bucket', '') == 'test_bucket'
    assert kwargs.get('Key', '') == 'test_path/obj3.json'
    ret = ss.search(key='item', value=json.dumps('y'), to_dict=True)
    assert len(ret) == 2
    assert isinstance(ret[0], dict)
    ret = ss.search(key='item', value=json.dumps('y'), projection=['serial_number'])
    assert len(ret) == 2
    assert isinstance(ret[0], dict)
    assert set(ret[0].keys()) == {'id', 'serial_number'}


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


@patch('boto3.client')
def test_s3_storage_save(mock_client, dummy_deliveries):
    config = Config()
    s3_config = config.get_config('s3')
    ss = S3Storage(s3=s3_config, item_type=Delivery, key='id', bucket='test_bucket', path='test_path')
    mock_client.return_value.exceptions.ClientError = Exception
    mock_client.return_value.head_object.return_value = None
    obj_key1 = dummy_deliveries[0].get_path(key='obj1', path='test_path', posix=True)
    obj_key2 = dummy_deliveries[1].get_path(key='obj2', path='test_path', posix=True)
    ss.save(item=dummy_deliveries[0])
    mock_client.return_value.put_object.assert_called_once_with(Body=dummy_deliveries[0].to_json(),
                                                                Bucket='test_bucket',
                                                                Key=obj_key1)
    mock_client.return_value.head_object.return_value = 'dummy'
    with pytest.raises(ValueError):
        ss.save(item=dummy_deliveries[1])
    ss.save(item=dummy_deliveries[1], update=True)
    mock_client.return_value.put_object.assert_called_with(Body=dummy_deliveries[1].to_json(),
                                                           Bucket='test_bucket',
                                                           Key=obj_key2)


@patch('boto3.client')
def test_s3_storage_update(mock_client, dummy_deliveries):
    config = Config()
    s3_config = config.get_config('s3')
    ss = S3Storage(s3=s3_config, item_type=Delivery, key='id', bucket='test_bucket', path='test_path')
    mock_client.return_value.exceptions.NoSuchKey = Exception
    mock_client.return_value.get_object.side_effect = partial(fake_get_object, dummy_deliveries)
    mock_client.return_value.exceptions.ClientError = Exception
    ss.update(key='obj2', values={'serial_number': 'test'})
    obj1 = dummy_deliveries[0]
    obj_key1 = obj1.get_path(key='obj1', path='test_path', posix=True)
    obj2 = dummy_deliveries[1]
    obj_key2 = obj2.get_path(key='obj2', path='test_path', posix=True)
    obj3 = dummy_deliveries[2]
    obj_key3 = obj3.get_path(key='obj3', path='test_path', posix=True)
    obj1_dict = obj1.to_dict()
    obj1_dict['serial_number'] = 'hello'
    obj1_dict['_id'] = 'hello'
    obj2_dict = obj2.to_dict()
    obj2_dict['serial_number'] = 'test'
    obj2_dict['_id'] = 'test'
    obj3_dict = obj3.to_dict()
    obj3_dict['serial_number'] = 'hello'
    obj3_dict['_id'] = 'hello'
    mock_client.return_value.put_object.assert_called_once_with(Bucket='test_bucket', Key=obj_key2,
                                                                Body=json.dumps(obj2_dict))
    ss.update(key=['obj1', 'obj3'], values={'serial_number': 'hello'})

    arg_list = mock_client.return_value.put_object.call_args_list
    assert len(arg_list) == 3
    assert arg_list[1][1] == {'Bucket': 'test_bucket', 'Key': obj_key1, 'Body': json.dumps(obj1_dict)}
    assert arg_list[2][1] == {'Bucket': 'test_bucket', 'Key': obj_key3, 'Body': json.dumps(obj3_dict)}


@patch('boto3.client')
def test_s3_storage_load(mock_client, dummy_deliveries):
    config = Config()
    s3_config = config.get_config('s3')
    ss = S3Storage(s3=s3_config, item_type=Delivery, key='id', bucket='test_bucket', path='test_path')
    mock_client.return_value.exceptions.NoSuchKey = Exception
    mock_client.return_value.get_object.side_effect = partial(fake_get_object, dummy_deliveries)
    ret = ss.load(key='obj2')
    assert isinstance(ret, Delivery)
    assert ret.to_dict() == dummy_deliveries[1].to_dict()
    ret = ss.load(key='obj1', to_dict=True)
    assert isinstance(ret, dict)
    assert ret == dummy_deliveries[0].to_dict()
    ret = ss.load(key='obj3', projection=['serial_number'])
    assert isinstance(ret, dict)
    assert set(ret.keys()) == {'id', 'serial_number'}
    ret = ss.load(key=['obj2', 'obj4', 'obj3'])
    assert len(ret) == 2
    assert {x.id for x in ret} == {'obj2', 'obj3'}
