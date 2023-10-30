from codepack.item import Item
from typing import Optional, Any, Dict
from datetime import datetime


class Lock(Item):
    def __init__(self,
                 key: str,
                 timestamp: Optional[float] = None) -> None:
        self.key = key
        self.timestamp: float = timestamp if timestamp else datetime.now().timestamp()
        super().__init__(name=key)

    def get_id(self) -> str:
        return self.key

    def retrieve(self) -> bool:
        return self.save()

    def __serialize__(self) -> Dict[str, Any]:
        return {'key': self.key, 'timestamp': self.timestamp}

    @classmethod
    def __deserialize__(cls, d: Dict[str, Any]) -> 'Lock':
        return cls(key=d['key'], timestamp=d['timestamp'])
