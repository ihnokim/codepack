from typing import List, Any
import abc


class AsyncMessageQueueMixin:
    @abc.abstractmethod
    async def receive(self, topic: str, group: str, batch: int = 10) -> List[Any]:
        pass  # pragma: no cover

    @abc.abstractmethod
    async def send(self, topic: str, message: Any) -> None:
        pass  # pragma: no cover
