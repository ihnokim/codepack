import abc
from typing import Any


class MessageSender(metaclass=abc.ABCMeta):
    def __init__(self) -> None:
        pass

    @abc.abstractmethod
    def init(self, *args: Any, **kwargs: Any) -> None:
        """initialize messenger"""

    @abc.abstractmethod
    def close(self) -> None:
        """close messenger"""

    @abc.abstractmethod
    def send(self, item: Any, *args: Any, **kwargs: Any) -> Any:
        """send item"""
