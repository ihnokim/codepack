from codepack.storages.messenger import Messenger
from codepack.utils.looper import Looper
from queue import Queue, Empty
from collections import namedtuple
from typing import Any, Callable


Message = namedtuple('Message', 'value')


class MemoryMessenger(Messenger):
    queues = dict()

    def __init__(self, topic: str, maxsize: int = 1000, batch: int = 10) -> None:
        super().__init__()
        self.topic = topic
        self.looper = None
        self.batch = batch
        self.init(topic=topic, maxsize=maxsize)

    @classmethod
    def init(cls, topic: str, maxsize: int = 1000) -> None:
        if topic not in cls.queues:
            cls.queues[topic] = Queue(maxsize=maxsize)

    @classmethod
    def __getitem__(cls, item: str) -> Queue:
        return cls.queues[item]

    def stop(self):
        if self.looper:
            self.looper.stop()

    def close(self) -> None:
        self.stop()

    @classmethod
    def destroy(cls):
        topics = list()
        for tp, q in cls.queues.items():
            with q.mutex:
                q.queue.clear()
            topics.append(tp)
        for topic in topics:
            cls.queues.pop(topic, None)

    def send(self, item: Any, *args: Any, **kwargs: Any) -> None:
        assert self.__class__.queues[self.topic] is not None
        self.__class__.queues[self.topic].put(item, block=False)

    def receive(self, callback: Callable, background: bool = False, interval: float = 1,
                *args: Any, **kwargs: Any) -> None:
        assert self.__class__.queues[self.topic] is not None
        self.looper = Looper(func=self.fetch, interval=interval, background=background, callback=callback,
                             *args, **kwargs)
        self.looper.start()

    def fetch(self, callback: Callable, *args: Any, **kwargs: Any) -> Any:
        buffer = None
        msgs = list()
        for i in range(self.batch):
            msg = None
            try:
                msg = self.__class__.queues[self.topic].get(block=False, *args, **kwargs)
            except Empty:
                continue
            if msg:
                msgs.append(Message(msg))
        if len(msgs) > 0:
            buffer = dict()
            buffer[self.topic] = msgs
        if buffer:
            return callback(buffer)
        else:
            return None
