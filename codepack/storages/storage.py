import abc
from typing import Dict, Any, List, Optional
from codepack.utils.configurable import Configurable


class Storage(Configurable, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def save(self, id: str, item: Dict[str, Any]) -> bool:
        pass  # pragma: no cover

    @abc.abstractmethod
    def load(self, id: str) -> Optional[Dict[str, Any]]:
        pass  # pragma: no cover

    @abc.abstractmethod
    def update(self, id: str, **kwargs: Any) -> bool:
        pass  # pragma: no cover

    @abc.abstractmethod
    def remove(self, id: str) -> bool:
        pass  # pragma: no cover

    @abc.abstractmethod
    def exists(self, id: str) -> bool:
        pass  # pragma: no cover

    @abc.abstractmethod
    def search(self, key: str, value: Any) -> List[str]:
        pass  # pragma: no cover

    @abc.abstractmethod
    def count(self, id: Optional[str]) -> int:
        pass  # pragma: no cover

    @abc.abstractmethod
    def list_all(self) -> List[str]:
        pass  # pragma: no cover

    @abc.abstractmethod
    async def list_like(self, id: str) -> List[str]:
        pass  # pragma: no cover

    @abc.abstractmethod
    def load_many(self, id: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        pass  # pragma: no cover

    @abc.abstractmethod
    def exists_many(self, id: List[str]) -> List[bool]:
        pass  # pragma: no cover
