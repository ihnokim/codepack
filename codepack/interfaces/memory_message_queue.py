from codepack.interfaces.interface import Interface
from queue import Queue, Empty
from typing import Any, Optional
from collections import namedtuple


Message = namedtuple('Message', 'value')


class MemoryMessageQueue(Interface):
    instances = {}

    @classmethod
    def get_instance(cls, key: str = 'default') -> Optional['MemoryMessageQueue']:
        if not cls.instance_exists(key=key):
            cls.instances[key] = cls()
        return cls.instances[key]

    @classmethod
    def instance_exists(cls, key: str):
        return key in cls.instances.keys()

    @classmethod
    def remove_instance(cls, key: str):
        if cls.instance_exists(key=key):
            cls.instances[key].close()
            cls.instances.pop(key, None)

    @classmethod
    def remove_all_instances(cls):
        keys = list(cls.instances.keys())
        for key in keys:
            cls.remove_instance(key=key)

    def __init__(self, config: Optional[dict] = None, *args, **kwargs) -> None:
        super().__init__(config if config else dict())
        self.maxsize = None
        self.connect(*args, **kwargs)

    def connect(self, *args: Any, **kwargs: Any) -> dict:
        self.maxsize = int(self.config['maxsize']) if 'maxsize' in self.config else 1000
        self.session = dict()
        return self.session

    def close(self) -> None:
        topics = list()
        for tp, q in self.session.items():
            with q.mutex:
                q.queue.clear()
            topics.append(tp)
        for topic in topics:
            self.session.pop(topic, None)
        self.session = None
        self._closed = True

    @classmethod
    def create_queue(cls, maxsize: int = 1000) -> Queue:
        return Queue(maxsize=maxsize)

    def __getitem__(self, topic: str) -> Queue:
        return self.session[topic]

    def create_topic(self, topic: str) -> None:
        if self.topic_exists(topic=topic):
            raise ValueError("topic '%s' already exists" % topic)
        else:
            self.session[topic] = self.create_queue(maxsize=self.maxsize)

    def topic_exists(self, topic: str) -> bool:
        return topic in self.session.keys()

    def remove_topic(self, topic: str) -> None:
        if self.topic_exists(topic=topic):
            with self.session[topic].mutex:
                self.session[topic].queue.clear()
            self.session.pop(topic, None)

    def fetch(self, topic: str, batch: int = 10, *args: Any, **kwargs: Any) -> list:
        assert self.topic_exists(topic=topic)
        msgs = list()
        for i in range(batch):
            try:
                msg = self.session[topic].get(block=False, *args, **kwargs)
                msgs.append(Message(msg))
            except Empty:
                continue
        return msgs

    def send(self, topic: str, item: Any, *args: Any, **kwargs: Any) -> None:
        assert self.topic_exists(topic=topic)
        self.session[topic].put(item, block=False, *args, **kwargs)
