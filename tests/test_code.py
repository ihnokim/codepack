from codepack.code import Code
from codepack.async_code import AsyncCode
from functools import partial
from tests import add2, mul2, add3, run_function
import pytest


test_params = ("code_class", [Code, AsyncCode])


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_partial_code_execution(code_class):
    plus1 = partial(add2, b=1)
    code = code_class(plus1)
    assert code.get_id() == 'add2'
    assert code.function.context == {'b': 1}
    assert await run_function(code.__call__, 3) == 4
    assert await run_function(code.__call__, 4, b=5) == 9
    with pytest.raises(TypeError):
        await run_function(code.__call__, 4, 5)


@pytest.mark.parametrize(*test_params)
def test_partial_code_serialization_without_metadata(code_class):
    plus1 = partial(add2, b=1)
    code = code_class(plus1)
    assert code.to_dict() == {'_id': code.get_id(),
                              'name': code.function.name,
                              'version': None,
                              'description': code.function.description,
                              'owner': None,
                              'function': code.function.to_dict()}


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_copying_partial_code_by_serialization(code_class):
    plus1 = partial(add2, b=1)
    code1 = code_class(plus1)
    code2 = code_class.from_dict(code1.to_dict())
    assert code1.to_dict() == code2.to_dict()
    assert await run_function(code2.__call__, 3) == 4


@pytest.mark.parametrize(*test_params)
def test_wrong_initialization_without_none_type_arguments(code_class):
    with pytest.raises(TypeError):
        _ = code_class()
    with pytest.raises(ValueError):
        _ = code_class(function=None)


@pytest.mark.parametrize(*test_params)
def test_cyclic_link_of_codes1(code_class):
    s = code_class(add2)
    with pytest.raises(ValueError):
        s > s


@pytest.mark.parametrize(*test_params)
def test_cyclic_link_of_codes2(code_class):
    s = code_class(add2)
    o = code_class(mul2)
    s > o
    assert s._is_acyclic(o)


@pytest.mark.parametrize(*test_params)
def test_cyclic_link_of_codes3(code_class):
    s = code_class(add2)
    o = code_class(mul2)
    o > s
    with pytest.raises(ValueError):
        s > o


@pytest.mark.parametrize(*test_params)
def test_cyclic_link_of_codes4(code_class):
    s = code_class(add2)
    o = code_class(mul2)
    x = code_class(add3)
    s > o > x
    assert s._is_acyclic(o)


@pytest.mark.parametrize(*test_params)
def test_cyclic_link_of_codes5(code_class):
    s = code_class(add2)
    o = code_class(mul2)
    x = code_class(add3)
    x > s > o
    assert s._is_acyclic(o)


@pytest.mark.parametrize(*test_params)
def test_cyclic_link_of_codes6(code_class):
    s = code_class(add2)
    o = code_class(mul2)
    x = code_class(add3)
    s > x > o
    assert s._is_acyclic(o)


@pytest.mark.parametrize(*test_params)
def test_cyclic_link_of_codes7(code_class):
    s = code_class(add2)
    o = code_class(mul2)
    x = code_class(add3)
    o > s > x
    with pytest.raises(ValueError):
        s > o


@pytest.mark.parametrize(*test_params)
def test_cyclic_link_of_codes8(code_class):
    s = code_class(add2)
    o = code_class(mul2)
    x = code_class(add3)
    x > o > s
    with pytest.raises(ValueError):
        s > o


@pytest.mark.parametrize(*test_params)
def test_cyclic_link_of_codes9(code_class):
    s = code_class(add2)
    o = code_class(mul2)
    x = code_class(add3)
    o > x > s
    with pytest.raises(ValueError):
        s > o
