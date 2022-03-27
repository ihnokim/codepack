from codepack.interface.kafka_consumer import KafkaConsumer
from codepack.interface.kafka_producer import KafkaProducer
from codepack.storage.messenger import Messenger
from typing import Union
from collections.abc import Callable


class KafkaMessenger(Messenger):
    def __init__(self, producer: Union[KafkaProducer, dict] = None, consumer: Union[KafkaConsumer, dict] = None,
                 producer_topic: str = None, consumer_topic: str = None,
                 producer_config: dict = None, consumer_config: dict = None):
        super().__init__()
        self.producer = None
        self.consumer = None
        self.producer_topic = None
        self.consumer_topic = None
        self.new_producer_connection = None
        self.new_consumer_connection = None
        self.init(producer=producer, consumer=consumer,
                  producer_topic=producer_topic, consumer_topic=consumer_topic,
                  producer_config=producer_config, consumer_config=consumer_config)

    def init(self, producer: Union[KafkaProducer, dict] = None, consumer: Union[KafkaConsumer, dict] = None,
             producer_topic: str = None, consumer_topic: str = None,
             producer_config: dict = None, consumer_config: dict = None):
        self.init_producer(producer=producer, producer_topic=producer_topic, producer_config=producer_config)
        self.init_consumer(consumer=consumer, consumer_topic=consumer_topic, consumer_config=consumer_config)

    def init_producer(self, producer: Union[KafkaProducer, dict] = None,
                      producer_topic: str = None, producer_config: dict = None):
        if isinstance(producer, KafkaProducer) or producer is None:
            self.producer = producer
            self.new_producer_connection = False
        elif isinstance(producer, dict):
            if not producer_config:
                producer_config = dict()
            if 'topic' in producer:
                self.producer_topic = producer['topic']
            self.producer = KafkaProducer(producer, **producer_config)
            self.new_producer_connection = True
        else:
            raise TypeError(type(producer))  # pragma: no cover
        if producer_topic is not None:
            self.producer_topic = producer_topic

    def init_consumer(self, consumer: Union[KafkaConsumer, dict] = None,
                      consumer_topic: str = None, consumer_config: dict = None):
        if isinstance(consumer, KafkaConsumer) or consumer is None:
            self.consumer = consumer
            self.new_consumer_connection = False
        elif isinstance(consumer, dict):
            if not consumer_config:
                consumer_config = dict()
            self.consumer = KafkaConsumer(consumer, **consumer_config)
            if 'topic' in consumer:
                self.consumer_topic = consumer['topic']
            self.new_consumer_connection = True
            if consumer_topic is not None:
                self.consumer_topic = consumer_topic
        else:
            raise TypeError(type(consumer))  # pragma: no cover

    def close(self):
        self.close_producer()
        self.close_consumer()

    def close_producer(self):
        if self.new_producer_connection:
            self.producer.close()
        self.producer = None

    def close_consumer(self):
        if self.new_consumer_connection:
            self.consumer.close()
        self.consumer = None

    def send(self, item: object, *args, **kwargs):
        assert self.producer is not None
        self.producer.produce(topic=self.producer_topic, value=item)

    def receive(self, callback: Callable, *args, **kwargs):
        assert self.consumer is not None
        self.consumer.consume(callback=callback, *args, **kwargs)
