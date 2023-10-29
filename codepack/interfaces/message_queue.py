from codepack.interfaces.interface import Interface
from typing import Any, List
from threading import Lock
import abc


class MessageQueue(Interface, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_group_key(self, topic: str, group: str = 'default') -> str:
        pass  # pragma: no cover

    @abc.abstractmethod
    def get_offset(self, topic: str, group: str = 'default') -> int:
        pass  # pragma: no cover

    @abc.abstractmethod
    def get_lock(self, topic: str, group: str = 'default') -> Lock:
        pass  # pragma: no cover

    @abc.abstractmethod
    def set_offset(self, topic: str, group: str = 'default', offset: int = 0) -> None:
        pass  # pragma: no cover

    def clear(self) -> None:
        pass  # pragma: no cover

    @abc.abstractmethod
    def create_topic(self, topic: str) -> None:
        pass  # pragma: no cover

    @abc.abstractmethod
    def list_topics(self) -> List[str]:
        pass  # pragma: no cover

    @abc.abstractmethod
    def topic_exists(self, topic: str) -> bool:
        pass  # pragma: no cover

    @abc.abstractmethod
    def remove_topic(self, topic: str) -> None:
        pass  # pragma: no cover

    @abc.abstractmethod
    def receive(self,
                topic: str,
                group: str,
                batch: int = 10) -> List[Any]:
        pass  # pragma: no cover

    @abc.abstractmethod
    def send(self, topic: str, message: Any) -> None:
        pass  # pragma: no cover
