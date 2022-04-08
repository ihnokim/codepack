from unittest.mock import patch
from codepack.interfaces import KafkaConsumer
from collections.abc import Callable


def add_1_and_raise_error(x):
    x['test_key'] += 1
    raise KeyboardInterrupt()


@patch('kafka.KafkaConsumer')
def test_kafka_consumer_init(mock_client):
    kafka_config = {'bootstrap_servers': 'ip1:9092,ip2:9092,ip3:9092'}
    kc = KafkaConsumer(config=kafka_config)
    arg_list = mock_client.call_args_list
    assert len(arg_list) == 1
    args, kwargs = arg_list[0]
    assert kwargs.get('bootstrap_servers', '') == 'ip1:9092,ip2:9092,ip3:9092'
    assert isinstance(kwargs.get('value_deserializer', ''), Callable)
    assert len(args) == 0
    assert kc.session is mock_client()
    kc.close()
    mock_client().close.assert_called_once()
    assert kc.closed()


@patch('kafka.KafkaConsumer')
def test_kafka_consumer_init_with_topic(mock_client):
    kafka_config = {'bootstrap_servers': 'ip1:9092,ip2:9092,ip3:9092', 'topic': 'test_topic'}
    kc = KafkaConsumer(config=kafka_config)
    arg_list = mock_client.call_args_list
    assert len(arg_list) == 1
    args, kwargs = arg_list[0]
    assert kwargs.get('bootstrap_servers', '') == 'ip1:9092,ip2:9092,ip3:9092'
    assert isinstance(kwargs.get('value_deserializer', ''), Callable)
    assert len(args) == 1 and args[0] == 'test_topic'
    assert kc.session is mock_client()
    kc.close()
    mock_client().close.assert_called_once()
    assert kc.closed()


@patch('kafka.KafkaConsumer')
def test_kafka_consumer_init_with_value_deserializer(mock_client):
    kafka_config = {'bootstrap_servers': 'ip1:9092,ip2:9092,ip3:9092', 'topic': 'test_topic',
                    'value_deserializer': lambda x: x}
    kc = KafkaConsumer(config=kafka_config)
    arg_list = mock_client.call_args_list
    assert len(arg_list) == 1
    args, kwargs = arg_list[0]
    assert kwargs.get('bootstrap_servers', '') == 'ip1:9092,ip2:9092,ip3:9092'
    assert isinstance(kwargs.get('value_deserializer', ''), Callable)
    assert kwargs.get('value_deserializer')('test') == 'test'
    assert len(args) == 1 and args[0] == 'test_topic'
    assert kc.session is mock_client()
    kc.close()
    mock_client().close.assert_called_once()
    assert kc.closed()


@patch('kafka.KafkaConsumer')
def test_kafka_consumer_consume(mock_client):
    kafka_config = {'bootstrap_servers': 'ip1:9092,ip2:9092,ip3:9092', 'topic': 'test_topic'}
    kc = KafkaConsumer(config=kafka_config)
    tmp = {'test_key': 3}
    kc.session.poll.return_value = tmp
    kc.consume(add_1_and_raise_error, timeout_ms=500)
    kc.session.poll.assert_called_once_with(timeout_ms=500)
    assert tmp['test_key'] == 4
    kc.close()
    mock_client().close.assert_called_once()
    assert kc.closed()
