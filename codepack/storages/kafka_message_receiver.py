from codepack.interfaces.kafka_consumer import KafkaConsumer
from codepack.storages.message_receiver import MessageReceiver
from typing import Union, Optional, Any, Callable


class KafkaMessageReceiver(MessageReceiver):
    def __init__(self, consumer: Optional[Union[KafkaConsumer, dict]] = None,
                 topic: Optional[str] = None,
                 *args: Any, **kwargs: Any) -> None:
        super().__init__()
        self.consumer = None
        self.topic = None
        self.new_connection = None
        self.init(consumer=consumer, topic=topic, *args, **kwargs)

    def init(self, consumer: Optional[Union[KafkaConsumer, dict]] = None,
             topic: Optional[str] = None, *args: Any, **kwargs: Any) -> None:
        if isinstance(consumer, KafkaConsumer) or consumer is None:
            self.consumer = consumer
            self.new_connection = False
        elif isinstance(consumer, dict):
            self.consumer = KafkaConsumer(consumer, *args, **kwargs)
            if 'topic' in consumer:
                self.topic = consumer['topic']
            self.new_connection = True
            if topic is not None:
                self.topic = topic
        else:
            raise TypeError(type(consumer))  # pragma: no cover

    def stop(self) -> None:
        if self.consumer:
            self.consumer.stop()

    def close(self) -> None:
        if self.new_connection:
            self.consumer.close()
        self.consumer = None

    def receive(self, callback: Callable, background: bool = False, interval: float = 1,
                *args: Any, **kwargs: Any) -> None:
        assert self.consumer is not None
        self.consumer.consume(callback=callback, background=background,
                              timeout_ms=int(interval * 1000), *args, **kwargs)
