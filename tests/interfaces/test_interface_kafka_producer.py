from unittest.mock import patch
from codepack.interfaces import KafkaProducer
from collections.abc import Callable
import pytest


@patch('kafka.KafkaProducer')
def test_kafka_producer_init(mock_client):
    kafka_config = {'bootstrap_servers': 'ip1:9092,ip2:9092,ip3:9092'}
    kp = KafkaProducer(config=kafka_config)
    arg_list = mock_client.call_args_list
    assert len(arg_list) == 1
    args, kwargs = arg_list[0]
    assert kwargs.get('bootstrap_servers', '') == 'ip1:9092,ip2:9092,ip3:9092'
    assert isinstance(kwargs.get('value_serializer', ''), Callable)
    assert len(args) == 0
    assert kp.session is mock_client()
    kp.close()
    mock_client().close.assert_called_once()
    assert kp.closed()


@patch('kafka.KafkaProducer')
def test_kafka_producer_init_with_value_serializer(mock_client):
    kafka_config = {'bootstrap_servers': 'ip1:9092,ip2:9092,ip3:9092', 'value_serializer': lambda x: x}
    kp = KafkaProducer(config=kafka_config)
    arg_list = mock_client.call_args_list
    assert len(arg_list) == 1
    args, kwargs = arg_list[0]
    assert kwargs.get('bootstrap_servers', '') == 'ip1:9092,ip2:9092,ip3:9092'
    assert isinstance(kwargs.get('value_serializer', ''), Callable)
    assert kwargs.get('value_serializer')('test') == 'test'
    assert kp.session is mock_client()
    kp.close()
    mock_client().close.assert_called_once()
    assert kp.closed()


@patch('kafka.KafkaProducer')
def test_kafka_producer_produce(mock_client):
    topic = 'test_topic'
    kafka_config = {'bootstrap_servers': 'ip1:9092,ip2:9092,ip3:9092'}
    kp = KafkaProducer(config=kafka_config)
    kp.produce(topic, value={'test_key1': 'test_value', 'test_key2': 123, 'test_key3': 1.23})
    client = mock_client()
    client.send.assert_called_once_with(topic, value={'test_key1': 'test_value',
                                                      'test_key2': 123, 'test_key3': 1.23})
    client.flush.assert_called_once()
    kp.close()
    client.close.assert_called_once()
    assert kp.closed()
    with pytest.raises(AssertionError):
        kp.produce()
    with pytest.raises(AssertionError):
        kp.dummy()


@patch('kafka.KafkaProducer')
def test_kafka_producer_getattr(mock_client):
    topic = 'test_topic'
    kafka_config = {'bootstrap_servers': 'ip1:9092,ip2:9092,ip3:9092'}
    kp = KafkaProducer(config=kafka_config)
    kp.produce(topic, value={'test_key1': 'test_value', 'test_key2': 123, 'test_key3': 1.23})
    client = mock_client()
    client.send.assert_called_once_with(topic, value={'test_key1': 'test_value',
                                                      'test_key2': 123, 'test_key3': 1.23})
    client.flush.assert_called_once()
    kp.bootstrap_connected()
    client.bootstrap_connected.assert_called_once()
    kp.close()
    client.close.assert_called_once()
    assert kp.closed()
    with pytest.raises(AssertionError):
        kp.produce(topic, value={'test_key1': 'test_value', 'test_key2': 123, 'test_key3': 1.23})
    with pytest.raises(AssertionError):
        kp.bootstrap_connected()
