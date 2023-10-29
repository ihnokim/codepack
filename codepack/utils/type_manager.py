from copy import deepcopy
from typing import Dict, Type
from codepack.utils.configurable import Configurable


class TypeManager:
    types = {}

    @classmethod
    def list_types(cls) -> Dict[str, Type[Configurable]]:
        return deepcopy(cls.types)

    @classmethod
    def get(cls, name: str) -> Type[Configurable]:
        return cls.types[name]

    @classmethod
    def register(cls, name: str, type: Type[Configurable]) -> None:
        cls.types[name] = type

    @classmethod
    def unregister(cls, name: str) -> None:
        cls.types.pop(name)
