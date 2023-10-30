import abc
from typing import Any, Callable
from codepack.utils.configurable import Configurable


class Receivable(Configurable, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def receive(self) -> Any:
        pass  # pragma: no cover

    @abc.abstractmethod
    def poll(self, callback: Callable[[Any], None], interval: float, background: bool = False) -> None:
        pass  # pragma: no cover

    @abc.abstractmethod
    def stop(self) -> None:
        pass  # pragma: no cover
