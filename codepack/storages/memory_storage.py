from typing import Dict, List, Optional, Any
from copy import deepcopy
from codepack.storages.storage import Storage
from codepack.interfaces.random_access_memory import RandomAccessMemory


class MemoryStorage(Storage):
    def __init__(self, keep_order: bool = False) -> None:
        super().__init__()
        self.interface = RandomAccessMemory(keep_order=keep_order)
        self.memory = self.interface.get_session()

    @classmethod
    def parse_config(cls, config: Dict[str, str]) -> Dict[str, Any]:
        return RandomAccessMemory.parse_config(config=config)

    def save(self, id: str, item: Dict[str, Any]) -> bool:
        if not self.exists(id=id):
            self.memory[id] = deepcopy(item)
            return True
        else:
            return False

    def load(self, id: str) -> Optional[Dict[str, Any]]:
        if self.exists(id=id):
            return deepcopy(self.memory[id])
        else:
            return None

    def update(self, id: str, **kwargs: Any) -> bool:
        if self.exists(id=id):
            for k, v in kwargs.items():
                self.memory[id][k] = v
            return True
        else:
            return False

    def remove(self, id: str) -> bool:
        if self.exists(id=id):
            self.memory.pop(id, None)
            return True
        else:
            return False

    def exists(self, id: str) -> bool:
        return id in self.memory.keys()

    def search(self, key: str, value: Any) -> List[str]:
        ret = list()
        for k, v in self.memory.items():
            if v[key] == value:
                ret.append(k)
        return ret

    def count(self, id: Optional[str] = None) -> int:
        if id:
            return len(self.list_like(id=id))
        else:
            return len(self.memory.keys())

    def list_all(self) -> List[str]:
        return list(self.memory.keys())

    def list_like(self, id: str) -> List[str]:
        ret = list()
        for k in self.memory.keys():
            if id in k:
                ret.append(k)
        return ret

    def load_many(self, id: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        if id:
            ret = list()
            for x in id:
                if self.exists(id=x):
                    ret.append(self.load(id=x))
            return ret
        else:
            return [deepcopy(x) for x in self.memory.values()]

    def exists_many(self, id: List[str]) -> List[bool]:
        ret = list()
        for i in id:
            ret.append(i in self.memory.keys())
        return ret
