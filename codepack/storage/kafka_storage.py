from codepack.interface import KafkaConsumer, KafkaProducer
from codepack.storage import Storage, Storable
from typing import Union
from typing import Type


class KafkaStorage(Storage):
    def __init__(self, item_type: Type[Storable] = None,
                 producer: Union[KafkaProducer, dict] = None, consumer: Union[KafkaConsumer, dict] = None,
                 topic: str = None, producer_config: dict = None, consumer_config: dict = None):
        super().__init__(item_type=item_type)
        self.producer = None
        self.consumer = None
        self.topic = None
        self.new_producer_connection = None
        self.new_consumer_connection = None
        self.init(producer=producer, consumer=consumer,
                  topic=topic, producer_config=producer_config, consumer_config=consumer_config)

    def init(self, producer: Union[KafkaProducer, dict] = None, consumer: Union[KafkaConsumer, dict] = None,
             topic: str = None, producer_config: dict = None, consumer_config: dict = None):
        self.topic = topic
        if isinstance(producer, KafkaProducer) or producer is None:
            self.producer = producer
            self.new_producer_connection = False
        elif isinstance(producer, dict):
            if not producer_config:
                producer_config = dict()
            self.producer = KafkaProducer(producer, **producer_config)
            self.new_producer_connection = True
        else:
            raise TypeError(type(producer))
        if isinstance(consumer, KafkaConsumer) or consumer is None:
            self.consumer = consumer
            self.new_consumer_connection = False
        elif isinstance(consumer, dict):
            if not consumer_config:
                consumer_config = dict()
            self.consumer = KafkaConsumer(consumer, **consumer_config)
            if 'topic' in consumer:
                self.topic = consumer['topic']
            self.new_consumer_connection = True
        else:
            raise TypeError(type(consumer))

    def close(self):
        if self.new_producer_connection:
            self.producer.close()
        self.producer = None
        if self.new_consumer_connection:
            self.consumer.close()
        self.consumer = None
