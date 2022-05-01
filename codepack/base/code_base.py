from codepack.storages.storable import Storable
from codepack.plugins.snapshots.snapshotable import Snapshotable
from collections.abc import Callable
from collections import OrderedDict
import abc
import re
import ast
import inspect
import dill
from typing import Optional
import typing


class CodeBase(Storable, Snapshotable, metaclass=abc.ABCMeta):
    def __init__(self, id: Optional[str] = None, serial_number: Optional[str] = None) -> None:
        Storable.__init__(self, id=id, serial_number=serial_number)
        Snapshotable.__init__(self)
        self.function = None
        self.source = None
        self.description = None

    @staticmethod
    def get_source(function: Callable) -> str:
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

    @staticmethod
    def get_function(source: str) -> Callable:
        pat = re.compile('^(\\s*def\\s.+[(].*[)].*[:])|(\\s*async\\s+def\\s.+[(].*[)].*[:])')
        assert pat.match(source), "'source' is not a function"
        tree = ast.parse(source, mode='exec')
        n_function = sum(isinstance(exp, ast.FunctionDef) for exp in tree.body)
        # needs to count all other instances, and assert that there is only one FunctionDef
        assert n_function == 1, "'source' should contain only one function."
        namespace = dict()
        _types = dir(typing)
        for _type in _types:
            _class = getattr(typing, _type)
            if hasattr(typing, '_Final'):
                _base = getattr(typing, '_Final')
            elif hasattr(typing, '_FinalTypingBase'):
                _base = getattr(typing, '_FinalTypingBase')
            else:
                _base = None
            if _base is not None and isinstance(_class, _base):
                namespace[_type] = _class
        # code = compile(tree, filename='blah', mode='exec')
        exec(source, namespace)
        return namespace[getattr(tree.body[0], 'name')]

    @staticmethod
    def get_reserved_params(function: Callable) -> OrderedDict:
        ret = OrderedDict()
        argspec = inspect.getfullargspec(function)
        defaults = dict(zip(argspec.args[-len(argspec.defaults):], argspec.defaults)) if argspec.defaults else dict()
        for arg in argspec.args:
            ret[arg] = defaults[arg] if arg in defaults else None
        for kwonlyarg in argspec.kwonlyargs:
            if argspec.kwonlydefaults is not None and kwonlyarg in argspec.kwonlydefaults:
                ret[kwonlyarg] = argspec.kwonlydefaults[kwonlyarg]
            else:
                ret[kwonlyarg] = None
        return ret
