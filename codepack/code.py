from typing import Any, Dict, Callable, Optional, Set, Union
from codepack.item import Item
from codepack.function import Function
from codepack.utils.linkable import Linkable
from codepack.utils.dependency import Dependency
from codepack.utils.observable import Observable
from queue import Queue
from functools import partial


class Code(Item, Linkable, Callable):
    def __init__(self,
                 function: Callable,
                 name: Optional[str] = None,
                 version: Optional[str] = None,
                 owner: Optional[str] = None,
                 description: Optional[str] = None):
        self.function: Function = self._get_function_object(function=function)
        self.upstream: Set['Code'] = set()
        self.downstream: Set['Code'] = set()
        self.dependencies: Dict[str, Dependency] = dict()
        self.observable = Observable()
        super().__init__(name=name if name else self.function.name,
                         version=version,
                         owner=owner,
                         description=description if description else self.function.description)

    @classmethod
    def _get_function_object(cls, function: Union[Callable, Dict[str, Any]]) -> Function:
        if isinstance(function, Function):
            return function
        elif isinstance(function, dict):
            return Function.from_dict(d=function)
        else:
            return Function(function=function)

    def get_id(self) -> str:
        return self.get_fullname()

    def __serialize__(self) -> Dict[str, Any]:
        return {'name': self.name,
                'version': self.version,
                'function': self.function.to_dict(),
                'owner': self.owner,
                'description': self.description}

    @classmethod
    def __deserialize__(cls, d: Dict[str, Any]) -> 'Code':
        return cls(name=d['name'],
                   version=d['version'],
                   function=cls._get_function_object(d['function']),
                   owner=d['owner'],
                   description=d['description'])

    def _is_acyclic(self, other: 'Code') -> bool:
        if self == other:
            return False
        q = Queue()
        q.put(other)
        while not q.empty():
            code = q.get()
            for child_code in code.downstream:
                if self == child_code:
                    return False
                q.put(child_code)
        return True

    def __shallow_link__(self, other: 'Code') -> None:
        if self._is_acyclic(other=other):
            self.downstream.add(other)
            other.upstream.add(self)
            self.observable.notify_observers()
        else:
            raise ValueError("Cycle detected")

    def _add_dependency(self, code: 'Code', param: str) -> None:
        code.dependencies[param] = self
        code.observable.notify_observers()

    def __deep_link__(self, other: 'Code') -> Dependency:
        self.__shallow_link__(other=other)
        return Dependency(on_update=partial(self._add_dependency, other))

    def __unlink__(self, other: 'Code') -> None:
        self.downstream.discard(other)
        other.upstream.discard(self)
        param = None
        for p, src in other.dependencies.items():
            if src == self:
                param = p
        other.dependencies.pop(param, None)
        self.observable.notify_observers()

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.function(*args, **kwargs)
