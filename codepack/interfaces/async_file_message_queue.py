from codepack.interfaces.file_message_queue import FileMessageQueue
from codepack.interfaces.async_file_interface import AsyncFileInterface
from codepack.asyncio.mixins.async_message_queue_mixin import AsyncMessageQueueMixin
from asyncio import Lock
from typing import Dict, List, Any
import os


class AsyncFileMessageQueue(AsyncMessageQueueMixin, FileMessageQueue):
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
            if os.path.exists(self.get_message_path(topic=topic)):
                for _ in range(batch):
                    lines = await AsyncFileInterface.readlines(path=self.get_message_path(topic=topic))
                    if offset < len(lines):
                        message = lines[offset]
                        messages.append(message[:-1])
                        offset += 1
                    else:
                        break
                self.set_offset(topic=topic, group=group, offset=offset)
        return messages

    async def send(self, topic: str, message: Any) -> None:
        assert self.topic_exists(topic=topic)
        await AsyncFileInterface.write(path=self.get_message_path(topic=topic), data=f'{message}\n', mode='a+')
