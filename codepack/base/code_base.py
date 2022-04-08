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
        # code = compile(tree, filename='blah', mode='exec')
        exec(source, namespace)
        return namespace[getattr(tree.body[0], 'name')]

    @staticmethod
    def get_args(function: Callable) -> OrderedDict:
        ret = OrderedDict()
        argspec = inspect.getfullargspec(function)
        args = argspec.args
        defaults = dict(zip(args[-len(argspec.defaults):], argspec.defaults)) if argspec.defaults else dict()
        for arg in args:
            if arg in defaults:
                ret[arg] = defaults[arg]
            else:
                ret[arg] = None
        return ret
