from codepack.messengers.receivable import Receivable
from codepack.interfaces.message_queue import MessageQueue
from codepack.interfaces.memory_message_queue import MemoryMessageQueue
from codepack.utils.looper import Looper
from codepack.utils.parser import Parser
from typing import Any, Callable, Dict, List, Optional
from copy import deepcopy


class Receiver(Receivable):
    def __init__(self,
                 topics: List[str],
                 group: str,
                 message_queue: Optional[MessageQueue] = None) -> None:
        super().__init__()
        self.looper = None
        self.topics = topics
        self.group = group
        if message_queue:
            self.mq = message_queue
        else:
            self.mq = MemoryMessageQueue()
        for topic in self.topics:
            if not self.mq.topic_exists(topic=topic):
                self.mq.create_topic(topic=topic)

    def stop(self) -> None:
        if self.looper:
            self.looper.stop()

    def close(self) -> None:
        self.stop()
        self.mq = None

    def poll(self, callback: Callable[[Any], None], interval: float, background: bool = False) -> None:
        self.looper = Looper(function=self._fetch_and_process_messages, callback=callback, interval=interval,
                             background=background)
        self.looper.start()

    def receive(self) -> List[Any]:
        messages = list()
        for topic in self.topics:
            messages += self.mq.receive(topic=topic, group=self.group)
        return messages

    def _fetch_and_process_messages(self, callback: Callable[[Any], None]) -> None:
        messages = self.receive()
        for message in messages:
            callback(message)

    @classmethod
    def parse_config(cls, config: Dict[str, str]) -> Dict[str, Any]:
        _config = deepcopy(config)
        if 'topics' in _config:
            _config['topics'] = Parser.parse_list(_config['topics'])
        return _config
