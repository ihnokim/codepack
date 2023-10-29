from codepack.interfaces.message_queue import MessageQueue
from typing import Dict, Any, List
from threading import Lock
from copy import deepcopy


# https://stackoverflow.com/questions/57753211/kafka-use-common-consumer-group-to-access-multiple-topics


class MemoryMessageQueue(MessageQueue):
    DELIMITER = '@'
    messages: Dict[str, list] = dict()
    offsets: Dict[str, int] = dict()
    locks: Dict[str, Lock] = dict()

    def __init__(self) -> None:
        self._is_closed = False
        self.connect()

    def connect(self) -> None:
        self._is_closed = False

    def get_session(self) -> 'MemoryMessageQueue':
        return self

    def get_group_key(self, topic: str, group: str = 'default') -> str:
        return f'{topic}{self.DELIMITER}{group}'

    def get_offset(self, topic: str, group: str = 'default') -> int:
        group_key = self.get_group_key(topic=topic, group=group)
        if group_key not in self.offsets:
            return 0
        else:
            return self.offsets[group_key]

    def get_lock(self, topic: str, group: str = 'default') -> Lock:
        group_key = self.get_group_key(topic=topic, group=group)
        if group_key not in self.locks:
            self.locks[group_key] = Lock()
        return self.locks[group_key]

    def set_offset(self, topic: str, group: str = 'default', offset: int = 0) -> None:
        group_key = self.get_group_key(topic=topic, group=group)
        self.offsets[group_key] = offset

    @classmethod
    def parse_config(cls, config: Dict[str, str]) -> Dict[str, Any]:
        return deepcopy(config)

    def clear(self) -> None:
        self.__class__.messages = dict()
        self.__class__.offsets = dict()
        self.__class__.locks = dict()

    def close(self) -> None:
        self._is_closed = True

    def is_closed(self):
        return self._is_closed

    def create_topic(self, topic: str, maxsize: int = 1000) -> None:
        if self.topic_exists(topic=topic):
            raise ValueError("topic '%s' already exists" % topic)
        else:
            self.messages[topic] = list()

    def list_topics(self) -> List[str]:
        return list(self.messages.keys())

    def topic_exists(self, topic: str) -> bool:
        return topic in self.messages.keys()

    def remove_topic(self, topic: str) -> None:
        if self.topic_exists(topic=topic):
            self.messages.pop(topic, None)
            offsets = [group_key for group_key in self.offsets if f'{topic}{self.DELIMITER}' in group_key]
            locks = [group_key for group_key in self.locks if f'{topic}{self.DELIMITER}' in group_key]
            for x in offsets:
                self.offsets.pop(x, None)
            for x in locks:
                self.locks.pop(x, None)

    def receive(self, topic: str, group: str, batch: int = 10) -> List[Any]:
        assert self.topic_exists(topic=topic)
        messages = list()
        with self.get_lock(topic=topic, group=group):
            offset = self.get_offset(topic=topic, group=group)
            for _ in range(batch):
                if offset < len(self.messages[topic]):
                    message = self.messages[topic][offset]
                    messages.append(message)
                    offset += 1
                else:
                    break
            self.set_offset(topic=topic, group=group, offset=offset)
        return messages

    def send(self, topic: str, message: Any) -> None:
        assert self.topic_exists(topic=topic)
        self.messages[topic].append(message)
