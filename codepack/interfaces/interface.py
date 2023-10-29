import abc
from typing import Any
from codepack.utils.configurable import Configurable


class Interface(Configurable, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def connect(self) -> None:
        pass  # pragma: no cover

    @abc.abstractmethod
    def close(self) -> None:
        pass  # pragma: no cover

    @abc.abstractmethod
    def is_closed(self) -> bool:
        pass  # pragma: no cover

    @abc.abstractmethod
    def get_session(self) -> Any:
        pass  # pragma: no cover
