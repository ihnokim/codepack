from codepack.storages.storable import Storable
from codepack.base.code_base import CodeBase
from functools import partial
from collections.abc import Callable
from typing import Optional, Any


class Callback(Storable, Callable):
    def __init__(self, function: Callable, context: Optional[dict] = None) -> None:
        Storable.__init__(self)
        Callable.__init__(self)
        self.function = None
        self.context = None
        self.set_function(function=function, context=context)

    def set_function(self, function: Callable, context: Optional[dict] = None) -> None:
        if context is None:
            self.context = dict()
        else:
            self.context = context
        if isinstance(function, partial):
            self.function = function.func
            for i, k in enumerate(CodeBase.get_args(self.function).keys()):
                if i >= len(function.args):
                    break
                if k not in self.context:
                    self.context[k] = function.args[i]
            for k, v in function.keywords.items():
                if k not in self.context:
                    self.context[k] = v
        elif isinstance(function, Callable):
            self.function = function
        else:
            raise TypeError(type(function))
        self.id = self.function.__name__

    def to_dict(self) -> dict:
        return {'source': CodeBase.get_source(self.function), 'context': self.context}

    @classmethod
    def from_dict(cls, d: dict) -> 'Callback':
        return cls(function=CodeBase.get_function(d['source']), context=d['context'])

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.function(*args, **kwargs, **self.context)
