import abc
from typing import Any


class AsyncSenderMixin:
    @abc.abstractmethod
    async def send(self, topic: str, message: Any) -> None:
        pass  # pragma: no cover
