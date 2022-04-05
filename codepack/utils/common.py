from collections.abc import Callable
from typing import Any, Optional, KeysView, ValuesView, ItemsView


class Common:
    def __init__(self) -> None:
        self.variables = dict()
        self._destroy = dict()

    def __getitem__(self, item: str) -> Any:
        return self.variables[item]

    def __getattr__(self, item: str) -> Any:
        return self.__getitem__(item)

    def keys(self) -> KeysView:
        return self.variables.keys()

    def values(self) -> ValuesView:
        return self.variables.values()

    def items(self) -> ItemsView:
        return self.variables.items()

    def add(self, key: str, value: Any, destroy: Optional[Callable] = None) -> None:
        self.variables[key] = value
        self._destroy[key] = destroy

    def remove(self, key: str) -> Optional[Any]:
        ret = None
        tmp = self.variables.pop(key)
        destroy = self._destroy.pop(key)
        if destroy and isinstance(destroy, Callable):
            ret = destroy(tmp)
        return ret

    def clear(self) -> None:
        keys = list(self.keys())
        for key in keys:
            self.remove(key)
