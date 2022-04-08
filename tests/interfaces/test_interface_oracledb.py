from unittest.mock import patch
from codepack.interfaces import OracleDB


@patch('cx_Oracle.connect')
def test_oracle_init(mock_client):
    oracle_config = {'host': 'localhost', 'port': 3306, 'service_name': 'hello', 'as_dict': 'False'}
    o = OracleDB(config=oracle_config, test_key='test_value')
    arg_list = mock_client.call_args_list
    assert len(arg_list) == 1
    args, kwargs = arg_list[0]
    assert kwargs.get('dsn', '') == '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=3306))' \
                                    '(CONNECT_DATA=(SERVICE_NAME=hello)))'
    assert kwargs.get('test_key', '') == 'test_value'
    assert isinstance(o.as_dict, bool) and not o.as_dict
    assert o.session is mock_client()
    o.close()
    mock_client().close.assert_called_once()
    assert o.closed()
    oracle_config2 = {'host': 'localhost', 'port': 3306, 'as_dict': 'True'}
    o2 = OracleDB(config=oracle_config2, test_key2='test_value2')
    arg_list = mock_client.call_args_list
    assert len(arg_list) > 1
    args, kwargs = arg_list[-1]
    assert kwargs.get('dsn', '') == '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=3306))' \
                                    '(CONNECT_DATA=))'
    assert kwargs.get('test_key2', '') == 'test_value2'
    assert isinstance(o2.as_dict, bool) and o2.as_dict


@patch('sshtunnel.SSHTunnelForwarder')
@patch('cx_Oracle.connect')
def test_oracle_init_with_sshtunnel_from_config_file(mock_client, mock_ssh):
    oracle_config = {'host': 'localhost', 'port': 3306, 'service_name': 'hello', 'as_dict': 'True',
                     'sshtunnel': 'config/test.ini:ssh'}
    o = OracleDB(config=oracle_config)
    assert o.ssh_config == {'ssh_host': 'localhost', 'ssh_port': '22', 'ssh_username': 'test', 'ssh_password': '1234'}
    mock_ssh.assert_called_once_with(('localhost', 22), remote_bind_address=('localhost', 3306),
                                     ssh_password='1234', ssh_username='test')
    arg_list = mock_client.call_args_list
    assert len(arg_list) == 1
    args, kwargs = arg_list[0]
    assert kwargs.get('dsn', '') == '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=127.0.0.1)(PORT=1))' \
                                    '(CONNECT_DATA=(SERVICE_NAME=hello)))'
    assert isinstance(o.as_dict, bool) and o.as_dict
    assert o.session is mock_client()
    o.close()
    mock_client().close.assert_called_once()
    mock_ssh().stop.assert_called_once()
    assert o.closed()


@patch('sshtunnel.SSHTunnelForwarder')
@patch('cx_Oracle.connect')
def test_oracle_init_with_sshtunnel_from_dict(mock_client, mock_ssh):
    oracle_config = {'host': 'localhost', 'port': 3306, 'service_name': 'hello', 'as_dict': 'False',
                     'sshtunnel': {'ssh_host': 'localhost', 'ssh_port': '22',
                                   'ssh_username': 'test', 'ssh_password': '1234'}}
    o = OracleDB(config=oracle_config)
    assert o.ssh_config == {'ssh_host': 'localhost', 'ssh_password': '1234', 'ssh_port': '22', 'ssh_username': 'test'}
    mock_ssh.assert_called_once_with(('localhost', 22), remote_bind_address=('localhost', 3306),
                                     ssh_password='1234', ssh_username='test')
    o.close()
    mock_client().close.assert_called_once()
    mock_ssh().stop.assert_called_once()
    assert o.closed()
