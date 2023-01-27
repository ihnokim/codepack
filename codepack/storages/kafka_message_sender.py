from codepack.interfaces.kafka_producer import KafkaProducer
from codepack.storages.message_sender import MessageSender
from typing import Union, Optional, Any


class KafkaMessageSender(MessageSender):
    def __init__(self, producer: Optional[Union[KafkaProducer, dict]] = None,
                 topic: Optional[str] = None, *args: Any, **kwargs: Any) -> None:
        super().__init__()
        self.producer = None
        self.topic = None
        self.new_connection = None
        self.init(producer=producer, topic=topic, *args, **kwargs)

    def init(self, producer: Optional[Union[KafkaProducer, dict]] = None,
             topic: Optional[str] = None, *args: Any, **kwargs: Any) -> None:
        if isinstance(producer, KafkaProducer) or producer is None:
            self.producer = producer
            self.new_connection = False
        elif isinstance(producer, dict):
            self.topic = producer.pop('topic', None)
            self.producer = KafkaProducer(producer, *args, **kwargs)
            self.new_connection = True
        else:
            raise TypeError(type(producer))  # pragma: no cover
        if topic is not None:
            self.topic = topic

    def close(self) -> None:
        if self.new_connection:
            self.producer.close()
        self.producer = None

    def send(self, item: Any, *args: Any, **kwargs: Any) -> None:
        assert self.producer is not None
        self.producer.produce(topic=self.topic, value=item, *args, **kwargs)
