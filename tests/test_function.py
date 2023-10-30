from codepack.function import Function
from codepack.async_function import AsyncFunction
from functools import partial
from tests import dummy_function, add2, async_add2, run_function
import pytest


test_params = ("function_class,function", [(Function, add2),
                                           (AsyncFunction, add2),
                                           (AsyncFunction, async_add2)])


def test_getting_reserved_params():
    function = Function(dummy_function)
    assert function.get_reserved_params() == [('a',), ('b', 2), ('c', None), ('d',), ('e', 3), ('f', None)]


def test_getting_type_annotations():
    function = Function(dummy_function)
    assert function.get_type_annotations() == {'a': "<class 'dict'>",
                                               'args': 'typing.Any',
                                               'b': "<class 'str'>",
                                               'd': 'typing.Any',
                                               'f': 'typing.Union[str, NoneType]',
                                               'kwargs': 'Code',
                                               'return': 'None'}


@pytest.mark.parametrize("function_class", [Function, AsyncFunction])
def test_initialization_with_wrong_type(function_class):
    function = 3
    with pytest.raises(TypeError):
        _ = function_class(function)


@pytest.mark.parametrize(*test_params)
def test_initialization_with_arg_partial_function(function_class, function):
    p = partial(function, 1)
    function = function_class(p)
    assert function.context == {'a': 1}


@pytest.mark.parametrize(*test_params)
def test_initialization_with_kwarg_partial_function(function_class, function):
    p = partial(function, b=2)
    function = function_class(p)
    assert function.context == {'b': 2}


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_running_with_arg_partial_function(function_class, function):
    p = partial(function, b=2)
    function = function_class(p)
    assert await run_function(function.__call__, a=3) == 5


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_running_with_kwarg_partial_function(function_class, function):
    p = partial(function, b=2)
    function = function_class(p)
    assert await run_function(function.__call__, a=3) == 5
