from typing import Dict, List, Optional, Any
from copy import deepcopy
from codepack.storages.memory_storage import MemoryStorage
from codepack.asyncio.mixins.async_storage_mixin import AsyncStorageMixin


class AsyncMemoryStorage(AsyncStorageMixin, MemoryStorage):
    async def save(self, id: str, item: Dict[str, Any]) -> bool:
        if not await self.exists(id=id):
            self.memory[id] = deepcopy(item)
            return True
        else:
            return False

    async def load(self, id: str) -> Optional[Dict[str, Any]]:
        if await self.exists(id=id):
            return deepcopy(self.memory[id])
        else:
            return None

    async def update(self, id: str, **kwargs: Any) -> bool:
        if await self.exists(id=id):
            for k, v in kwargs.items():
                self.memory[id][k] = v
            return True
        else:
            return False

    async def remove(self, id: str) -> bool:
        if await self.exists(id=id):
            self.memory.pop(id, None)
            return True
        else:
            return False

    async def exists(self, id: str) -> bool:
        return id in self.memory.keys()

    async def search(self, key: str, value: Any) -> List[str]:
        ret = list()
        for k, v in self.memory.items():
            if v[key] == value:
                ret.append(k)
        return ret

    async def count(self, id: Optional[str] = None) -> int:
        if id:
            return len(await self.list_like(id=id))
        else:
            return len(self.memory.keys())

    async def list_all(self) -> List[str]:
        return list(self.memory.keys())

    async def list_like(self, id: str) -> List[str]:
        ret = list()
        for k in self.memory.keys():
            if id in k:
                ret.append(k)
        return ret

    async def load_many(self, id: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        if id:
            ret = list()
            for x in id:
                if await self.exists(id=x):
                    ret.append(await self.load(id=x))
            return ret
        else:
            return [deepcopy(x) for x in self.memory.values()]

    async def exists_many(self, id: List[str]) -> List[bool]:
        ret = list()
        for i in id:
            ret.append(i in self.memory.keys())
        return ret
