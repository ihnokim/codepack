from unittest.mock import patch
from codepack.interfaces import MySQL
import pytest
from pymysql.cursors import Cursor, DictCursor


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
                    'cursorclass': 'pymysql.cursors.Cursor', 'sshtunnel': 'config/test.ini:ssh'}
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
    rows2 = [{'test_key1': 'test_value4', 'test_key2': 1.23, 'test_key3': 123},
             {'test_key1': 'test_value5', 'test_key2': 2.34, 'test_key3': 234},
             {'test_key1': 'test_value6', 'test_key2': 3.45, 'test_key3': 345}]
    m.insert_many(db='codepack', table='table1', rows=rows2, commit=True)
    assert mock_client().cursor().execute.call_count == 6
    assert mock_client().cursor().fetchall.call_count == 2
    assert mock_client().cursor().close.call_count == 2
    mock_client().commit.assert_called_once()
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
