from unittest.mock import patch
from codepack.interface import DynamoDB, MySQL, MSSQL
import pytest
from botocore.config import Config
from decimal import Decimal
from pymysql.cursors import Cursor, DictCursor


@patch('boto3.client')
def test_dynamodb_init(mock_client):
    d = DynamoDB(config={'test_key1': 'test_value1', 'test_key2': 'test_value2'})
    arg_list = mock_client.call_args_list
    assert len(arg_list) == 1
    args, kwargs = arg_list[0]
    assert 'config' in kwargs and 'test_key1' in kwargs and 'test_key2' in kwargs
    assert isinstance(kwargs['config'], Config)
    assert kwargs['test_key1'] == 'test_value1'
    assert kwargs['test_key2'] == 'test_value2'
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


@patch('pymysql.connect')
def test_mysql_init(mock_client):
    mysql_config = {'host': 'localhost', 'port': 3306, 'user': 'admin', 'passwd': 'test', 'charset': 'utf8',
                    'cursorclass': 'pymysql.cursors.DictCursor'}
    m = MySQL(config=mysql_config)
    arg_list = mock_client.call_args_list
    assert len(arg_list) == 1
    args, kwargs = arg_list[0]
    assert kwargs.get('host', '') == 'localhost'
    assert kwargs.get('port', '') == 3306
    assert kwargs.get('user', '') == 'admin'
    assert kwargs.get('passwd', '') == 'test'
    assert kwargs.get('charset', '') == 'utf8'
    assert 'cursorclass' in kwargs and isinstance(kwargs['cursorclass'], DictCursor.__class__)
    assert m.session is mock_client()
    m.close()
    mock_client().close.assert_called_once()
    assert m.closed()


@patch('sshtunnel.SSHTunnelForwarder')
@patch('pymysql.connect')
def test_mysql_init_with_sshtunnel_from_config_file(mock_client, mock_ssh):
    mysql_config = {'host': 'localhost', 'port': 3306, 'user': 'admin', 'passwd': 'test', 'charset': 'utf8',
                    'cursorclass': 'pymysql.cursors.Cursor', 'sshtunnel': 'config/test_conn.ini:ssh'}
    m = MySQL(config=mysql_config)
    assert m.ssh_config == {'ssh_host': 'localhost', 'ssh_port': '22', 'ssh_username': 'test', 'ssh_password': '1234'}
    mock_ssh.assert_called_once_with(('localhost', 22), remote_bind_address=('localhost', 3306),
                                     ssh_password='1234', ssh_username='test')
    arg_list = mock_client.call_args_list
    assert len(arg_list) == 1
    args, kwargs = arg_list[0]
    assert kwargs.get('host', '') == '127.0.0.1'
    assert kwargs.get('port', '') != 3306
    assert kwargs.get('user', '') == 'admin'
    assert kwargs.get('passwd', '') == 'test'
    assert kwargs.get('charset', '') == 'utf8'
    assert 'cursorclass' in kwargs and isinstance(kwargs['cursorclass'], Cursor.__class__)
    assert m.session is mock_client()
    m.close()
    mock_client().close.assert_called_once()
    mock_ssh().stop.assert_called_once()
    assert m.closed()


@patch('sshtunnel.SSHTunnelForwarder')
@patch('pymysql.connect')
def test_mysql_init_with_sshtunnel_from_dict(mock_client, mock_ssh):
    mysql_config = {'host': 'localhost', 'port': 3306, 'user': 'admin', 'passwd': 'test', 'charset': 'utf8',
                    'cursorclass': 'pymysql.cursors.Cursor',
                    'sshtunnel': {'ssh_host': 'localhost', 'ssh_port': '22',
                                  'ssh_username': 'test', 'ssh_password': '1234'}}
    m = MySQL(config=mysql_config)
    assert m.ssh_config == {'ssh_host': 'localhost', 'ssh_password': '1234', 'ssh_port': '22', 'ssh_username': 'test'}
    mock_ssh.assert_called_once_with(('localhost', 22), remote_bind_address=('localhost', 3306),
                                     ssh_password='1234', ssh_username='test')
    m.close()
    mock_client().close.assert_called_once()
    mock_ssh().stop.assert_called_once()
    assert m.closed()


@patch('pymysql.connect')
def test_mysql_select(mock_client):
    mysql_config = {'host': 'localhost', 'port': 3306, 'user': 'admin', 'passwd': 'test', 'charset': 'utf8',
                    'cursorclass': 'pymysql.cursors.DictCursor'}
    m = MySQL(config=mysql_config)
    m.select(db='codepack', table='table1', projection=['c1', 'c2', 'c3'],
             test_key1='test_value1', test_key2=1.23, test_key3=123)
    mock_client().cursor.assert_called_once()
    mock_client().cursor().execute.assert_called_once_with("select c1,c2,c3 from codepack.table1 "
                                                           "where test_key1 = 'test_value1' "
                                                           "and test_key2 = 1.23 and test_key3 = 123")
    mock_client().cursor().fetchall.assert_called_once()
    mock_client().cursor().close.assert_called_once()
    mock_client().commit.assert_not_called()
    mock_client().rollback.assert_not_called()
    mock_client().cursor.return_value = None
    with pytest.raises(AttributeError):
        m.select(db='codepack', table='table1', projection=['c1', 'c2', 'c3'],
                 test_key1='test_value1', test_key2=1.23, test_key3=123)
    mock_client().rollback.assert_called_once()


@patch('pymysql.connect')
def test_mysql_insert(mock_client):
    mysql_config = {'host': 'localhost', 'port': 3306, 'user': 'admin', 'passwd': 'test', 'charset': 'utf8',
                    'cursorclass': 'pymysql.cursors.DictCursor'}
    m = MySQL(config=mysql_config)
    m.insert(db='codepack', table='table1',
             test_key1='test_value1', test_key2=1.23, test_key3=123)
    mock_client().cursor.assert_called_once()
    mock_client().cursor().execute.assert_called_once_with("replace into "
                                                           "codepack.table1 (test_key1, test_key2, test_key3) "
                                                           "values ('test_value1', 1.23, 123)")
    mock_client().cursor().fetchall.assert_called_once()
    mock_client().cursor().close.assert_called_once()
    mock_client().commit.assert_not_called()
    mock_client().rollback.assert_not_called()
    m.insert(db='codepack', table='table1',
             test_key1='test_value1', test_key2=1.23, test_key3=123, update=False)
    mock_client().cursor().execute.assert_called_with("insert into "
                                                      "codepack.table1 (test_key1, test_key2, test_key3) "
                                                      "values ('test_value1', 1.23, 123)")
    assert mock_client().cursor().fetchall.call_count == 2
    assert mock_client().cursor().close.call_count == 2
    mock_client().commit.assert_not_called()
    mock_client().rollback.assert_not_called()
    m.insert(db='codepack', table='table1',
             test_key1='test_value1', test_key2=1.23, test_key3=123, update=False, commit=True)
    mock_client().commit.assert_called_once()
    mock_client().cursor.return_value = None
    with pytest.raises(AttributeError):
        m.insert(db='codepack', table='table1',
                 test_key1='test_value1', test_key2=1.23, test_key3=123)
    mock_client().rollback.assert_called_once()


@patch('pymysql.connect')
def test_mysql_insert_many(mock_client):
    mysql_config = {'host': 'localhost', 'port': 3306, 'user': 'admin', 'passwd': 'test', 'charset': 'utf8',
                    'cursorclass': 'pymysql.cursors.DictCursor'}
    m = MySQL(config=mysql_config)
    rows = [{'test_key1': 'test_value1', 'test_key2': 1.23, 'test_key3': 123},
            {'test_key1': 'test_value2', 'test_key2': 2.34, 'test_key3': 234},
            {'test_key1': 'test_value3', 'test_key2': 3.45, 'test_key3': 345}]
    m.insert_many(db='codepack', table='table1', rows=rows)
    mock_client().cursor.assert_called_once()
    assert mock_client().cursor().execute.call_count == 3
    mock_client().cursor().fetchall.assert_called_once()
    mock_client().cursor().close.assert_called_once()
    mock_client().commit.assert_not_called()
    mock_client().rollback.assert_not_called()
    mock_client().cursor.return_value = None
    with pytest.raises(AttributeError):
        m.insert_many(db='codepack', table='table1', rows=rows)
    mock_client().rollback.assert_called_once()


@patch('pymysql.connect')
def test_mysql_delete(mock_client):
    mysql_config = {'host': 'localhost', 'port': 3306, 'user': 'admin', 'passwd': 'test', 'charset': 'utf8',
                    'cursorclass': 'pymysql.cursors.DictCursor'}
    m = MySQL(config=mysql_config)
    m.delete(db='codepack', table='table1',
             test_key1='test_value1', test_key2=1.23, test_key3=123, test_key4=['a', 'b', 'c'])
    mock_client().cursor.assert_called_once()
    mock_client().cursor().execute.assert_called_once_with("delete from codepack.table1 "
                                                           "where test_key1 = 'test_value1' and test_key2 = 1.23 and "
                                                           "test_key3 = 123 and test_key4 in ('a', 'b', 'c')")
    mock_client().cursor().fetchall.assert_called_once()
    mock_client().cursor().close.assert_called_once()
    mock_client().commit.assert_not_called()
    m.delete(db='codepack', table='table1',
             test_key1='test_value1', test_key2=1.23, test_key3=123, test_key4=['a', 'b', 'c'], commit=True)
    assert mock_client().cursor().fetchall.call_count == 2
    assert mock_client().cursor().close.call_count == 2
    mock_client().commit.assert_called_once()
    mock_client().rollback.assert_not_called()
    mock_client().cursor.return_value = None
    with pytest.raises(AttributeError):
        m.delete(db='codepack', table='table1',
                 test_key1='test_value1', test_key2=1.23, test_key3=123, test_key4=['a', 'b', 'c'])
    mock_client().rollback.assert_called_once()


@patch('pymysql.connect')
def test_mysql_call(mock_client):
    mysql_config = {'host': 'localhost', 'port': 3306, 'user': 'admin', 'passwd': 'test', 'charset': 'utf8',
                    'cursorclass': 'pymysql.cursors.DictCursor'}
    m = MySQL(config=mysql_config)
    m.call(db='codepack', procedure='test_procedure', test_key1='test_value1', test_key2=1.23, test_key3=123)
    mock_client().cursor.assert_called_once()
    mock_client().cursor().execute.assert_called_once_with("call codepack.test_procedure("
                                                           "@test_key1 := 'test_value1', "
                                                           "@test_key2 := '1.23', "
                                                           "@test_key3 := '123')")
    mock_client().cursor().fetchall.assert_called_once()
    mock_client().cursor().close.assert_called_once()
    mock_client().commit.assert_not_called()
    mock_client().rollback.assert_not_called()
    mock_client().cursor.return_value = None
    with pytest.raises(AttributeError):
        m.call(db='codepack', procedure='test_procedure', test_key1='test_value1', test_key2=1.23, test_key3=123)
    mock_client().rollback.assert_called_once()


@patch('pymssql.connect')
def test_mssql_init(mock_client):
    mssql_config = {'host': 'localhost', 'port': 3306, 'user': 'admin', 'password': 'test', 'charset': 'utf8',
                    'as_dict': 'False'}
    m = MSSQL(config=mssql_config)
    arg_list = mock_client.call_args_list
    assert len(arg_list) == 1
    args, kwargs = arg_list[0]
    assert kwargs.get('host', '') == 'localhost'
    assert kwargs.get('port', '') == 3306
    assert kwargs.get('user', '') == 'admin'
    assert kwargs.get('password', '') == 'test'
    assert kwargs.get('charset', '') == 'utf8'
    assert 'as_dict' in kwargs and isinstance(kwargs['as_dict'], bool)
    assert not kwargs.get('as_dict', True)
    assert m.session is mock_client()
    m.close()
    mock_client().close.assert_called_once()
    assert m.closed()


@patch('sshtunnel.SSHTunnelForwarder')
@patch('pymssql.connect')
def test_mssql_init_with_sshtunnel_from_config_file(mock_client, mock_ssh):
    mssql_config = {'host': 'localhost', 'port': 3306, 'user': 'admin', 'password': 'test', 'charset': 'utf8',
                    'as_dict': 'True', 'sshtunnel': 'config/test_conn.ini:ssh'}
    m = MSSQL(config=mssql_config)
    assert m.ssh_config == {'ssh_host': 'localhost', 'ssh_port': '22', 'ssh_username': 'test', 'ssh_password': '1234'}
    mock_ssh.assert_called_once_with(('localhost', 22), remote_bind_address=('localhost', 3306),
                                     ssh_password='1234', ssh_username='test')
    arg_list = mock_client.call_args_list
    assert len(arg_list) == 1
    args, kwargs = arg_list[0]
    assert kwargs.get('host', '') == '127.0.0.1'
    assert kwargs.get('port', '') != 3306
    assert kwargs.get('user', '') == 'admin'
    assert kwargs.get('password', '') == 'test'
    assert kwargs.get('charset', '') == 'utf8'
    assert 'as_dict' in kwargs and isinstance(kwargs['as_dict'], bool)
    assert kwargs.get('as_dict', False)
    assert m.session is mock_client()
    m.close()
    mock_client().close.assert_called_once()
    mock_ssh().stop.assert_called_once()
    assert m.closed()


@patch('sshtunnel.SSHTunnelForwarder')
@patch('pymssql.connect')
def test_mssql_init_with_sshtunnel_from_dict(mock_client, mock_ssh):
    mssql_config = {'host': 'localhost', 'port': 3306, 'user': 'admin', 'password': 'test', 'charset': 'utf8',
                    'as_dict': 'False',
                    'sshtunnel': {'ssh_host': 'localhost', 'ssh_port': '22',
                                  'ssh_username': 'test', 'ssh_password': '1234'}}
    m = MSSQL(config=mssql_config)
    assert m.ssh_config == {'ssh_host': 'localhost', 'ssh_password': '1234', 'ssh_port': '22', 'ssh_username': 'test'}
    mock_ssh.assert_called_once_with(('localhost', 22), remote_bind_address=('localhost', 3306),
                                     ssh_password='1234', ssh_username='test')
    m.close()
    mock_client().close.assert_called_once()
    mock_ssh().stop.assert_called_once()
    assert m.closed()


@patch('pymssql.connect')
def test_mssql_select(mock_client):
    mssql_config = {'host': 'localhost', 'port': 3306, 'user': 'admin', 'password': 'test', 'charset': 'utf8',
                    'as_dict': 'False'}
    m = MSSQL(config=mssql_config)
    mock_client().cursor.return_value.rowcount = -1
    m.select(db='codepack', table='table1', projection=['c1', 'c2', 'c3'],
             test_key1='test_value1', test_key2=1.23, test_key3=123, test_key4=['a', 'b', 'c'])
    mock_client().cursor.assert_called_once()
    mock_client().cursor().execute.assert_called_once_with("select c1,c2,c3 from codepack.table1 "
                                                           "where test_key1 = 'test_value1' "
                                                           "and test_key2 = 1.23 and test_key3 = 123 "
                                                           "and test_key4 in ('a', 'b', 'c')")
    mock_client().cursor().fetchall.assert_called_once()
    mock_client().cursor().close.assert_called_once()
    mock_client().commit.assert_not_called()
    mock_client().rollback.assert_not_called()
    mock_client().cursor.return_value = None
    with pytest.raises(AttributeError):
        m.select(db='codepack', table='table1', projection=['c1', 'c2', 'c3'],
                 test_key1='test_value1', test_key2=1.23, test_key3=123, test_key4=['a', 'b', 'c'])
    mock_client().rollback.assert_called_once()
