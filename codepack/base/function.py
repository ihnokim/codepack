import codepack.utils.functions
from typing import Optional, Callable
from functools import partial


class Function:
    def __init__(self,
                 function: Optional[Callable] = None,
                 source: Optional[str] = None,
                 context: Optional[dict] = None) -> None:
        self.function = None
        self.source = None
        self.description = None
        self.context = None
        self.set_function(function=function, source=source, context=context)

    def set_function(self,
                     function: Optional[Callable] = None,
                     source: Optional[str] = None,
                     context: Optional[dict] = None) -> None:
        if context:
            self.context = context
        else:
            self.context = dict()
        if source:
            source = source.strip()
            self.function = codepack.utils.functions.get_function(source)
            self.source = source
        elif function:
            if isinstance(function, partial):
                self.function = function.func
                for i, k in enumerate(codepack.utils.functions.get_reserved_params(self.function).keys()):
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
            self.source = codepack.utils.functions.get_source(self.function)
        else:
            raise AssertionError("either 'function' or 'source' should not be None")
        self.description = self.function.__doc__.strip() if self.function.__doc__ is not None else str()
