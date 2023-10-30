from codepack.interfaces.memory_message_queue import MemoryMessageQueue
from codepack.asyncio.mixins.async_message_queue_mixin import AsyncMessageQueueMixin
from asyncio import Lock
from typing import Dict, List, Any


class AsyncMemoryMessageQueue(AsyncMessageQueueMixin, MemoryMessageQueue):
    locks: Dict[str, Lock] = dict()

    def get_lock(self, topic: str, group: str = 'default') -> Lock:
        group_key = self.get_group_key(topic=topic, group=group)
        if group_key not in self.locks:
            self.locks[group_key] = Lock()
        return self.locks[group_key]

    async def receive(self, topic: str, group: str, batch: int = 10) -> List[Any]:
        assert self.topic_exists(topic=topic)
        messages = list()
        async with self.get_lock(topic=topic, group=group):
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

    async def send(self, topic: str, message: Any) -> None:
        assert self.topic_exists(topic=topic)
        self.messages[topic].append(message)
