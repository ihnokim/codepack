from codepack.messengers.sendable import Sendable
from codepack.interfaces.message_queue import MessageQueue
from codepack.interfaces.memory_message_queue import MemoryMessageQueue
from typing import Any, Dict, Optional
from copy import deepcopy


class Sender(Sendable):
    def __init__(self,
                 message_queue: Optional[MessageQueue] = None) -> None:
        if message_queue:
            self.mq = message_queue
        else:
            self.mq = MemoryMessageQueue()

    def send(self, topic: str, message: Any) -> None:
        self.mq.send(topic=topic, message=message)

    @classmethod
    def parse_config(cls, config: Dict[str, str]) -> Dict[str, Any]:
        return deepcopy(config)  # pragma: no cover
