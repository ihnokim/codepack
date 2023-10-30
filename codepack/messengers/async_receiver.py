from codepack.messengers.receiver import Receiver
from codepack.asyncio.mixins.async_receiver_mixin import AsyncReceiverMixin
from codepack.interfaces.async_memory_message_queue import AsyncMemoryMessageQueue
from codepack.asyncio.mixins.async_message_queue_mixin import AsyncMessageQueueMixin
from typing import Any, Callable, List, Optional
import asyncio


class AsyncReceiver(AsyncReceiverMixin, Receiver):
    def __init__(self,
                 topics: List[str],
                 group: str,
                 message_queue: Optional[AsyncMessageQueueMixin] = None) -> None:
        if message_queue:
            mq = message_queue
        else:
            mq = AsyncMemoryMessageQueue()
        self.stop_event = asyncio.Event()
        super().__init__(topics=topics,
                         group=group,
                         message_queue=mq)

    async def stop(self) -> None:
        if not self.stop_event.is_set():
            self.stop_event.set()

    async def close(self) -> None:
        await self.stop()
        self.mq = None

    async def poll(self, callback: Callable[[Any], None], interval: float, background: bool = False) -> None:
        while not self.stop_event.is_set():
            try:
                async for messages in self.receive():
                    await asyncio.sleep(interval)
                    for message in messages:
                        await callback(message)
            except KeyboardInterrupt:
                self.stop_event.set()
                break

    async def receive(self) -> List[Any]:
        messages = list()
        for topic in self.topics:
            messages += await self.mq.receive(topic=topic, group=self.group)
        yield messages
