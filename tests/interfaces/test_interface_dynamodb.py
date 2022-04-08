from unittest.mock import patch
from codepack.interfaces import DynamoDB
import pytest
from botocore.config import Config
from decimal import Decimal


@patch('boto3.client')
def test_dynamodb_init(mock_client):
    d = DynamoDB(config={'test_key1': 'test_value1', 'test_key2': 'test_value2'})
    arg_list = mock_client.call_args_list
    assert len(arg_list) == 1
    args, kwargs = arg_list[0]
    assert 'config' in kwargs and 'test_key1' in kwargs and 'test_key2' in kwargs
    assert kwargs['test_key1'] == 'test_value1'
    assert kwargs['test_key2'] == 'test_value2'
    assert isinstance(kwargs['config'], Config)
    assert kwargs['config'].retries == {'max_attempts': 3}
    assert 'config' in d.config and d.config['config'] == kwargs['config']
    assert d.session is mock_client()
    d.close()
    assert d.closed()


def test_dynamodb_array_parser():
    s1 = '1/3/5/7/9'
    ret = DynamoDB.array_parser(s1, sep='/', dtype=int)
    assert ret == [1, 3, 5, 7, 9]
    s2 = '1/3//5/7/9'
    with pytest.raises(ValueError):
        DynamoDB.array_parser(s2, sep='/', dtype=float)
    ret = DynamoDB.array_parser(s1, sep='/', dtype=float)
    assert ret == [1.0, 3.0, 5.0, 7.0, 9.0]


@patch('boto3.client')
def test_dynamodb_list_tables(mock_client):
    d = DynamoDB(config={})
    mock_client.assert_called_once()
    d.list_tables('test')
    mock_client().list_tables.assert_called_once_with(ExclusiveStartTableName='test')


@patch('boto3.client')
def test_dynamodb_describe_tables(mock_client):
    d = DynamoDB(config={})
    d.describe_table('test')
    mock_client().describe_table.assert_called_once_with(TableName='test')


@patch('boto3.client')
def test_dynamodb_query(mock_client):
    d = DynamoDB(config={})
    d.session.query.return_value = {'Items': [{'s_key': {'S': 's_value'}},
                                              {'ss_key': {'SS': ['a', 'b', 'c']}},
                                              {'n_key1': {'N': '1.23'}},
                                              {'n_key2': {'N': '123'}}]}
    ret = d.query(table='codepack', q="test_key = 'test_value'", columns=['c1', 'c2'])
    assert ret == [{'s_key': 's_value'}, {'ss_key': {'a', 'b', 'c'}},
                   {'n_key1': Decimal('1.23')}, {'n_key2': Decimal(123)}]
    mock_client().query.assert_called_once_with(KeyConditionExpression="test_key = 'test_value'",
                                                ProjectionExpression='c1,c2', TableName='codepack')


@patch('boto3.client')
def test_dynamodb_select(mock_client):
    d = DynamoDB(config={})
    d.session.query.return_value = dict()
    d.select(table='codepack', columns=['c1', 'c2', 'c3'], test_key1='test_value1', test_key2=123, test_key3=1.23)
    mock_client().query.assert_called_once_with(KeyConditionExpression=
                                                "test_key1 = 'test_value1' and test_key2 = 123 and test_key3 = 1.23",
                                                ProjectionExpression='c1,c2,c3', TableName='codepack')
