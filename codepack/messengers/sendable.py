import abc
from typing import Any
from codepack.utils.configurable import Configurable


class Sendable(Configurable, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def send(self, topic: str, message: Any) -> None:
        pass  # pragma: no cover
