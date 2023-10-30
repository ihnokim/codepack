import abc
from typing import Any, Callable


class AsyncReceiverMixin:
    @abc.abstractmethod
    async def receive(self) -> Any:
        pass  # pragma: no cover

    @abc.abstractmethod
    async def poll(self, callback: Callable[[Any], None], interval: float, background: bool = False) -> None:
        pass  # pragma: no cover

    @abc.abstractmethod
    async def stop(self) -> None:
        pass  # pragma: no cover
