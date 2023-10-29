from codepack.interfaces.message_queue import MessageQueue
from codepack.interfaces.file_interface import FileInterface
from typing import Dict, Any, List
import os
from threading import Lock
from copy import deepcopy


class FileMessageQueue(MessageQueue):
    DELIMITER = '@'
    MESSAGE_FILE = 'm'
    locks: Dict[str, Lock] = dict()

    def __init__(self, path: str) -> None:
        self._is_closed = False
        self.path = path
        self.make_worksapce()
        self.connect()

    def connect(self) -> None:
        self._is_closed = False

    def get_session(self) -> 'FileMessageQueue':
        return self

    def make_worksapce(self) -> None:
        FileInterface.mkdir(path=self.path)

    def get_group_key(self, topic: str, group: str = 'default') -> str:
        return os.path.join(self.path, topic, group)

    def get_offset(self, topic: str, group: str = 'default') -> int:
        group_key = self.get_group_key(topic=topic, group=group)
        if not FileInterface.exists(path=group_key):
            return 0
        else:
            with open(group_key, 'r') as f:
                s = int(f.readline())
            return s

    def get_lock(self, topic: str, group: str = 'default') -> Lock:
        group_key = self.get_group_key(topic=topic, group=group)
        if group_key not in self.locks:
            self.locks[group_key] = Lock()
        return self.locks[group_key]

    def set_offset(self, topic: str, group: str = 'default', offset: int = 0) -> None:
        group_key = self.get_group_key(topic=topic, group=group)
        with open(group_key, 'w') as f:
            f.write(str(offset))

    @classmethod
    def parse_config(cls, config: Dict[str, str]) -> Dict[str, Any]:
        return deepcopy(config)

    def clear(self) -> None:
        FileInterface.rmdir(path=self.path)
        self.__class__.locks = dict()

    def close(self) -> None:
        self._is_closed = True

    def is_closed(self):
        return self._is_closed

    def create_topic(self, topic: str) -> None:
        if self.topic_exists(topic=topic):
            raise ValueError("topic '%s' already exists" % topic)
        else:
            FileInterface.mkdir(path=os.path.join(self.path, topic))

    def list_topics(self) -> List[str]:
        paths = FileInterface.listdir(path=self.path)
        topics = list()
        for path in paths:
            if os.path.isdir(os.path.join(self.path, path)):
                topics.append(path)
        return topics

    def topic_exists(self, topic: str) -> bool:
        return topic in self.list_topics()

    def remove_topic(self, topic: str) -> None:
        if self.topic_exists(topic=topic):
            FileInterface.rmdir(os.path.join(self.path, topic))
            locks = [gk for gk in self.locks if f'{self.get_group_key(topic=topic, group="")}' in gk]
            for x in locks:
                self.locks.pop(x, None)

    def receive(self, topic: str, group: str, batch: int = 10) -> List[Any]:
        assert self.topic_exists(topic=topic)
        messages = list()
        with self.get_lock(topic=topic, group=group):
            offset = self.get_offset(topic=topic, group=group)
            if os.path.exists(self.get_message_path(topic=topic)):
                for _ in range(batch):
                    with open(self.get_message_path(topic=topic), 'r') as f:
                        lines = f.readlines()
                    if offset < len(lines):
                        message = lines[offset]
                        messages.append(message[:-1])
                        offset += 1
                    else:
                        break
                self.set_offset(topic=topic, group=group, offset=offset)
        return messages

    def send(self, topic: str, message: Any) -> None:
        assert self.topic_exists(topic=topic)
        with open(self.get_message_path(topic=topic), 'a') as f:
            f.write(f'{message}\n')

    def get_message_path(self, topic: str) -> str:
        return os.path.join(self.path, topic, f'{self.MESSAGE_FILE}0')
