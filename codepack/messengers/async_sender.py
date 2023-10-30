from codepack.messengers.sender import Sender
from codepack.asyncio.mixins.async_sender_mixin import AsyncSenderMixin
from codepack.interfaces.async_memory_message_queue import AsyncMemoryMessageQueue
from codepack.asyncio.mixins.async_message_queue_mixin import AsyncMessageQueueMixin
from typing import Any, Optional


class AsyncSender(AsyncSenderMixin, Sender):
    def __init__(self,
                 message_queue: Optional[AsyncMessageQueueMixin] = None) -> None:
        if message_queue:
            mq = message_queue
        else:
            mq = AsyncMemoryMessageQueue()
        super().__init__(message_queue=mq)

    async def send(self, topic: str, message: Any) -> None:
        await self.mq.send(topic=topic, message=message)
