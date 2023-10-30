from typing import Optional, Any, Dict, List, Tuple
from functools import partial
from collections.abc import Callable
import re
import ast
import inspect
import dill
import typing
from codepack.item import Item
from copy import deepcopy


class Function(Item, Callable):
    def __init__(self,
                 function: Optional[Callable] = None,
                 source: Optional[str] = None,
                 context: Optional[dict] = None,
                 name: Optional[str] = None,
                 description: Optional[str] = None) -> None:
        self.function: Callable
        self.source: str
        self.context: Dict[str, Any]
        self._set_context(context=context)
        if source:
            self._init_by_source(source=source)
        elif function:
            self._init_by_function(function=function)
        else:
            raise ValueError("either 'function' or 'source' should not be None")
        self.name: str = name if name else self.function.__name__
        self.description: str = description if description else (self.function.__doc__.strip()
                                                                 if self.function.__doc__ is not None
                                                                 else str())

    def get_id(self) -> str:
        return self.name

    def __serialize__(self) -> Dict[str, Any]:
        return {'name': self.name,
                'description': self.description,
                'source': self.source.strip(),
                'context': deepcopy(self.context)}

    @classmethod
    def __deserialize__(cls, d: Dict[str, Any]) -> 'Function':
        return cls(name=d['name'],
                   description=d['description'],
                   source=d['source'],
                   context=d['context'])

    @classmethod
    def get_function(cls, source: str) -> Callable:
        pat = re.compile('^(\\s*def\\s.+[(].*[)].*[:])|(\\s*async\\s+def\\s.+[(].*[)].*[:])')
        assert pat.match(source), "'source' is not a function"
        tree = ast.parse(source, mode='exec')
        n_function = 0
        for exp in tree.body:
            if isinstance(exp, ast.FunctionDef) or isinstance(exp, ast.AsyncFunctionDef):
                n_function += 1
        # needs to count all other instances, and assert that there is only one FunctionDef
        assert n_function == 1, "'source' should contain only one function."
        namespace = dict()
        _types = dir(typing)
        for _type in _types:
            _class = getattr(typing, _type)
            if hasattr(typing, '_Final'):  # pragma: no cover
                _base = getattr(typing, '_Final')  # pragma: no cover
            elif hasattr(typing, '_FinalTypingBase'):  # pragma: no cover
                _base = getattr(typing, '_FinalTypingBase')  # pragma: no cover
            else:  # pragma: no cover
                _base = None  # pragma: no cover
            if _base is not None and isinstance(_class, _base):
                namespace[_type] = _class
        # code = compile(tree, filename='blah', mode='exec')
        exec(source, namespace)
        return namespace[getattr(tree.body[0], 'name')]

    @classmethod
    def get_source(cls, function: Callable) -> str:
        assert isinstance(function, Callable), "'function' should be an instance of Callable"
        assert function.__name__ != '<lambda>', "Invalid function '<lambda>'"
        assert hasattr(function, '__code__'), "'function' should have an attribute '__code__'"
        assert getattr(function, '__code__').co_filename != '<string>', "'function' should not be defined in <string>"
        source = None
        for test in [inspect.getsource, dill.source.getsource]:
            try:
                source = test(function)
            except Exception:  # pragma: no cover
                pass  # pragma: no cover
            if source is not None:
                break
        return source

    def _init_by_function(self, function: Callable) -> None:
        if isinstance(function, partial):
            self.function = function.func
            for i, k in enumerate(self.get_reserved_params()):
                param = k[0]
                if i >= len(function.args):
                    break
                if param not in self.context:
                    self.context[param] = function.args[i]
            for k, v in function.keywords.items():
                if k not in self.context:
                    self.context[k] = v
        elif isinstance(function, Callable):
            self.function = function
        else:
            raise TypeError(type(function))
        self.source = self.get_source(self.function)

    def _init_by_source(self, source: str) -> None:
        source = source.strip()
        self.function = self.get_function(source)
        self.source = source

    def _set_context(self, context: Optional[dict] = None) -> None:
        if context:
            self.context = context
        else:
            self.context = dict()

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        for k, v in self.context.items():
            if k not in kwargs:
                kwargs[k] = v
        return self.function(*args, **kwargs)

    def get_argspec(self) -> inspect.FullArgSpec:
        return inspect.getfullargspec(self.function)

    def get_type_annotations(self) -> Dict[str, str]:
        return {k: str(v) for k, v in self.get_argspec().annotations.items()}

    def get_reserved_params(self) -> List[Tuple]:
        ret = list()
        argspec = self.get_argspec()
        defaults = dict(zip(argspec.args[-len(argspec.defaults):], argspec.defaults)) if argspec.defaults else dict()
        for arg in argspec.args:
            if arg in defaults:
                ret.append((arg, defaults[arg]))
            else:
                ret.append((arg,))
        for kwonlyarg in argspec.kwonlyargs:
            if argspec.kwonlydefaults is not None and kwonlyarg in argspec.kwonlydefaults:
                ret.append((kwonlyarg, argspec.kwonlydefaults[kwonlyarg]))
            else:
                ret.append((kwonlyarg,))
        return ret
