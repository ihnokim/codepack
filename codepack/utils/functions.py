from collections.abc import Callable
from collections import OrderedDict
from typing import Union
import re
import ast
import inspect
import dill
import typing


def inform_supervisor_of_termination(x: dict, supervisor: Union[str, 'codepack.plugins.supervisor.Supervisor']) -> None:  # noqa: F821, E501
    import requests
    from codepack.plugins.supervisor import Supervisor
    if x['state'] == 'TERMINATED':
        if isinstance(supervisor, str):
            requests.get(supervisor + '/organize/%s' % x['serial_number'])
        elif isinstance(supervisor, Supervisor):
            supervisor.organize(x['serial_number'])


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
