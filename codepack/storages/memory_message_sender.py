from codepack.storages.message_sender import MessageSender
from codepack.interfaces.memory_message_queue import MemoryMessageQueue
from collections import namedtuple
from typing import Any, Optional, Union


Message = namedtuple('Message', 'value')


class MemoryMessageSender(MessageSender):
    def __init__(self, topic: str, message_queue: Optional[Union[MemoryMessageQueue, str]] = None,
                 *args: Any, **kwargs: Any) -> None:
        super().__init__()
        self.topic = None
        self.mq = None
        self.init(message_queue=message_queue, topic=topic, *args, **kwargs)

    def init(self, topic: str, message_queue: Optional[Union[MemoryMessageQueue, str]] = None,
             *args: Any, **kwargs: Any) -> None:
        if message_queue is None:
            self.mq = MemoryMessageQueue.get_instance()
        elif isinstance(message_queue, MemoryMessageQueue):
            self.mq = message_queue
        elif isinstance(message_queue, str):
            self.mq = MemoryMessageQueue.get_instance(key=message_queue)
        else:
            raise TypeError(type(message_queue))  # pragma: no cover
        self.topic = topic
        if not self.mq.topic_exists(topic=self.topic):
            self.mq.create_topic(topic=self.topic)

    def close(self) -> None:
        self.mq = None

    def send(self, item: Any, *args: Any, **kwargs: Any) -> None:
        self.mq.send(topic=self.topic, item=item)
