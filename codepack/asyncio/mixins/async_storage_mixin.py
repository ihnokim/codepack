import abc
from typing import Dict, Any, List, Optional


class AsyncStorageMixin:
    @abc.abstractmethod
    async def save(self, id: str, item: Dict[str, Any]) -> bool:
        pass  # pragma: no cover

    @abc.abstractmethod
    async def load(self, id: str) -> Optional[Dict[str, Any]]:
        pass  # pragma: no cover

    @abc.abstractmethod
    async def update(self, id: str, **kwargs: Any) -> bool:
        pass  # pragma: no cover

    @abc.abstractmethod
    async def remove(self, id: str) -> bool:
        pass  # pragma: no cover

    @abc.abstractmethod
    async def exists(self, id: str) -> bool:
        pass  # pragma: no cover

    @abc.abstractmethod
    async def search(self, key: str, value: Any) -> List[str]:
        pass  # pragma: no cover

    @abc.abstractmethod
    async def count(self, id: Optional[str]) -> int:
        pass  # pragma: no cover

    @abc.abstractmethod
    async def list_all(self) -> List[str]:
        pass  # pragma: no cover

    @abc.abstractmethod
    async def list_like(self, id: str) -> List[str]:
        pass  # pragma: no cover

    @abc.abstractmethod
    async def load_many(self, id: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        pass  # pragma: no cover

    @abc.abstractmethod
    async def exists_many(self, id: List[str]) -> List[bool]:
        pass  # pragma: no cover
