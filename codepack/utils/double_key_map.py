from typing import Dict, Tuple, Iterator, Optional, Iterable
from copy import deepcopy


class DoubleKeyMap:
    def __init__(self,
                 map: Optional[Dict[str, Optional[str]]] = None,
                 delimiter: str = '->'):
        self.delimiter = delimiter
        self.map: Dict[str, Optional[str]] = dict()
        if map:
            for k, v in map.items():
                if delimiter not in k:
                    raise ValueError(f"Delimiter '{self.delimiter}' not found")
                self.map[k] = v

    def get_key(self, key1: str, key2: str) -> str:
        return f'{key1}{self.delimiter}{key2}'

    def keys(self) -> Iterable[Tuple[str, str]]:
        return [self.parse_key(k) for k in self.map.keys()]

    def values(self) -> Iterable[Optional[str]]:
        return [v for v in self.map.values()]

    def items(self) -> Iterable[Tuple[str, str, Optional[str]]]:
        return [(*self.parse_key(k), v) for k, v in self.map.items()]

    def parse_key(self, key: str) -> Tuple[str, str]:
        return tuple(key.split(self.delimiter))

    def put(self, key1: str, key2: str, value: Optional[str] = None) -> None:
        key = self.get_key(key1=key1, key2=key2)
        self.map[key] = value

    def contains(self, key1: str, key2: str) -> bool:
        key = self.get_key(key1=key1, key2=key2)
        return key in self.map.keys()

    def remove(self, key1: str, key2: str) -> None:
        key = self.get_key(key1=key1, key2=key2)
        self.map.pop(key)

    def remove_all(self) -> None:
        self.map.clear()

    def __iter__(self) -> Iterator[str]:
        return self.map.__iter__()

    def to_dict(self) -> Dict[str, Optional[str]]:
        return deepcopy(self.map)

    def __len__(self) -> int:
        return len(self.map)
