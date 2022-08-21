from codepack.storages.message_receiver import MessageReceiver
from codepack.interfaces.memory_message_queue import MemoryMessageQueue
from codepack.utils.looper import Looper
from collections import namedtuple
from typing import Any, Callable, Optional, Union


Message = namedtuple('Message', 'value')


class MemoryMessageReceiver(MessageReceiver):
    def __init__(self, topic: str, message_queue: Optional[Union[MemoryMessageQueue, str]] = None, batch: int = 10,
                 *args: Any, **kwargs: Any) -> None:
        super().__init__()
        self.topic = None
        self.looper = None
        self.batch = None
        self.mq = None
        self.init(message_queue=message_queue, topic=topic, batch=batch, *args, **kwargs)

    def init(self, topic: str, message_queue: Optional[Union[MemoryMessageQueue, str]] = None, batch: int = 10,
             *args: Any, **kwargs: Any) -> None:
        if message_queue is None:
            self.mq = MemoryMessageQueue.get_instance()
        elif isinstance(message_queue, MemoryMessageQueue):
            self.mq = message_queue
        elif isinstance(message_queue, str):
            self.mq = MemoryMessageQueue.get_instance(key=message_queue)
        else:
            raise TypeError(type(message_queue))  # pragma: no cover
        if not self.mq.topic_exists(topic=self.topic):
            self.mq.create_topic(topic=self.topic)
        self.batch = batch
        self.topic = topic

    def stop(self):
        if self.looper:
            self.looper.stop()

    def close(self) -> None:
        self.stop()
        self.mq = None

    def receive(self, callback: Callable, background: bool = False, interval: float = 1,
                *args: Any, **kwargs: Any) -> None:
        assert self.mq.topic_exists(topic=self.topic)
        self.looper = Looper(func=self._fetch_and_process_messages, interval=interval,
                             background=background, callback=callback, *args, **kwargs)
        self.looper.start()

    def _fetch_and_process_messages(self, callback: Callable, *args: Any, **kwargs: Any) -> Any:
        msgs = self.mq.fetch(topic=self.topic, batch=self.batch, *args, **kwargs)
        if len(msgs) > 0:
            buffer = dict()
            buffer[self.topic] = msgs
            return callback(buffer)
        else:
            return None
