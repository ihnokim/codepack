from codepack.storages.storable import Storable
from codepack.base.function import Function
from collections.abc import Callable
from typing import Optional, Any


class Callback(Function, Storable):
    def __init__(self,
                 function: Optional[Callable] = None,
                 source: Optional[str] = None,
                 context: Optional[dict] = None,
                 id: Optional[str] = None) -> None:
        Function.__init__(self, function=function, source=source, context=context)
        Storable.__init__(self, id=id)
        Callable.__init__(self)
        if self.get_id() is None:
            self.set_id(self.function.__name__)

    def to_dict(self) -> dict:
        return {'id': self.get_id(), 'source': self.source, 'context': self.context}

    @classmethod
    def from_dict(cls, d: dict) -> 'Callback':
        return cls(id=d['id'], source=d['source'], context=d['context'])

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.function(*args, **kwargs, **self.context)
