from codepack.code import Code
from codepack.async_code import AsyncCode
from codepack.codepack import CodePack
from codepack.async_codepack import AsyncCodePack
from codepack.arg import Arg
from codepack.argpack import ArgPack
from unittest import mock
from tests import add2, mul2, add3, combination, linear, print_x, get_codepack_from_code, raise_error, run_function
import pytest


test_params = ("code_class,codepack_class", [(Code, CodePack),
                                             (AsyncCode, AsyncCodePack)])


@pytest.mark.parametrize(*test_params)
def test_linking_codes_without_codepack(code_class, codepack_class):
    code1 = code_class(add2)
    code2 = code_class(mul2)
    code1 > code2
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 1
    assert len(code2.upstream) == 1
    assert len(code2.downstream) == 0


@pytest.mark.parametrize(*test_params)
def test_code_propagation_by_initializer(code_class, codepack_class):
    code1 = code_class(add2)
    code2 = code_class(mul2)
    code1 > code2
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 1
    assert len(code2.upstream) == 1
    assert len(code2.downstream) == 0
    codepack = codepack_class(codes=[code1])
    assert len(codepack.codes) == 2
    assert len(codepack.links) == 1


@pytest.mark.parametrize(*test_params)
def test_code_propagation_by_update(code_class, codepack_class):
    code1 = code_class(add2)
    code2 = code_class(mul2)
    codepack = codepack_class(codes=[code1])
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 0
    assert len(code2.upstream) == 0
    assert len(code2.downstream) == 0
    assert len(codepack.codes) == 1
    assert len(codepack.links) == 0
    code1 > code2
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 1
    assert len(code2.upstream) == 1
    assert len(code2.downstream) == 0
    assert len(codepack.codes) == 2
    assert len(codepack.links) == 1


@pytest.mark.parametrize(*test_params)
def test_reversed_code_propagation_by_update(code_class, codepack_class):
    code1 = code_class(add2)
    code2 = code_class(mul2)
    codepack = codepack_class(codes=[code1])
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 0
    assert len(code2.upstream) == 0
    assert len(code2.downstream) == 0
    assert len(codepack.codes) == 1
    assert len(codepack.links) == 0
    code2 < code1
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 1
    assert len(code2.upstream) == 1
    assert len(code2.downstream) == 0
    assert len(codepack.codes) == 2
    assert len(codepack.links) == 1


@pytest.mark.parametrize(*test_params)
def test_chaining_code_propagation_by_initializer1(code_class, codepack_class):
    code1 = code_class(add2)
    code2 = code_class(mul2)
    code3 = code_class(add3)
    code1 > code2 > code3
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 1
    assert len(code2.upstream) == 1
    assert len(code2.downstream) == 1
    assert len(code3.upstream) == 1
    assert len(code3.downstream) == 0
    codepack = codepack_class(codes=[code2])
    assert len(codepack.codes) == 3
    assert len(codepack.links) == 2


@pytest.mark.parametrize(*test_params)
def test_chaining_code_propagation_by_initializer2(code_class, codepack_class):
    code1 = code_class(add2)
    code2 = code_class(mul2)
    code3 = code_class(add3)
    code1 > code2 > code3
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 1
    assert len(code2.upstream) == 1
    assert len(code2.downstream) == 1
    assert len(code3.upstream) == 1
    assert len(code3.downstream) == 0
    codepack = codepack_class(codes=[code3])
    assert len(codepack.codes) == 3
    assert len(codepack.links) == 2


@pytest.mark.parametrize(*test_params)
def test_chaining_code_propagation_by_update(code_class, codepack_class):
    code1 = code_class(add2)
    code2 = code_class(mul2)
    code3 = code_class(add3)
    codepack = codepack_class(codes=[code1])
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 0
    assert len(code2.upstream) == 0
    assert len(code2.downstream) == 0
    assert len(code3.upstream) == 0
    assert len(code3.downstream) == 0
    assert len(codepack.codes) == 1
    assert len(codepack.links) == 0
    code1 > code2 > code3
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 1
    assert len(code2.upstream) == 1
    assert len(code2.downstream) == 1
    assert len(code3.upstream) == 1
    assert len(code3.downstream) == 0
    assert len(codepack.codes) == 3
    assert len(codepack.links) == 2


@pytest.mark.parametrize("codepack_class", [CodePack, AsyncCodePack])
def test_codepack_without_any_code(codepack_class):
    with pytest.raises(ValueError):
        _ = codepack_class(codes=[])


@pytest.mark.parametrize(*test_params)
def test_if_code_removal_is_possbile_when_codes_are_linked(code_class, codepack_class):
    code1 = code_class(add2)
    code2 = code_class(mul2)
    code1 >> code2 | 'a'
    codepack = codepack_class(codes=[code2])
    assert get_codepack_from_code(code=code1) == codepack
    codepack.remove_code(code=code1)
    assert get_codepack_from_code(code=code1) == codepack
    assert code1.get_id() in codepack.codes.keys()


@pytest.mark.parametrize(*test_params)
def test_if_code_removal_is_possbile_when_codes_are_not_linked(code_class, codepack_class):
    code1 = code_class(add2)
    code2 = code_class(mul2)
    code1 >> code2 | 'a'
    codepack = codepack_class(codes=[code2])
    assert get_codepack_from_code(code=code1) == codepack
    code1 / code2
    codepack.remove_code(code=code1)
    assert get_codepack_from_code(code=code1) is None
    assert code1.get_id() not in codepack.codes.keys()
    assert len(codepack.codes) == 1
    assert len(codepack.links) == 0


@pytest.mark.parametrize(*test_params)
def test_if_code_removal_is_possbile_when_some_codes_are_linked(code_class, codepack_class):
    code1 = code_class(add2)
    code2 = code_class(mul2)
    code3 = code_class(add3)
    code1 > code2 > code3
    codepack = codepack_class(codes=[code2])
    code1 / code2
    codepack.remove_code(code=code2)
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 0
    assert len(code2.upstream) == 0
    assert len(code2.downstream) == 1
    assert len(code3.upstream) == 1
    assert len(code3.downstream) == 0
    assert len(codepack.codes) == 3
    assert len(codepack.links) == 1


@pytest.mark.parametrize(*test_params)
def test_if_code_removal_is_possbile_when_some_codes_are_not_linked(code_class, codepack_class):
    code1 = code_class(add2)
    code2 = code_class(mul2)
    code3 = code_class(add3)
    code1 > code2 > code3
    codepack = codepack_class(codes=[code2])
    code1 / code2
    codepack.remove_code(code=code1)
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 0
    assert len(code2.upstream) == 0
    assert len(code2.downstream) == 1
    assert len(code3.upstream) == 1
    assert len(code3.downstream) == 0
    assert len(codepack.codes) == 2
    assert len(codepack.links) == 1


@pytest.mark.parametrize(*test_params)
def test_if_error_occurs_when_user_tries_to_link_codes_from_different_codepacks(code_class, codepack_class):
    code1 = code_class(add2)
    code2 = code_class(mul2)
    _ = codepack_class(codes=[code1])
    _ = codepack_class(codes=[code2])
    with pytest.raises(ValueError):
        code1 > code2


@pytest.mark.parametrize(*test_params)
def test_if_adding_links_updates_codepack_composition_automatically(code_class, codepack_class):
    code1 = code_class(add2)
    code2 = code_class(mul2)
    code3 = code_class(add3)
    codepack = codepack_class(codes=[code1])
    assert len(codepack.codes) == 1
    assert len(codepack.links) == 0
    code1 > code2 > code3
    assert len(codepack.codes) == 3
    assert len(codepack.links) == 2


@pytest.mark.parametrize(*test_params)
def test_if_adding_code_dependency_automatically_updates_codepack(code_class, codepack_class):
    code1 = code_class(add2)
    code2 = code_class(mul2)
    codepack1 = codepack_class(codes=[code1])
    assert len(codepack1.codes) == 1
    assert len(codepack1.links) == 0
    code1 >> code2 | 'a'
    assert len(codepack1.codes) == 2
    assert len(codepack1.links) == 1
    assert list(codepack1.links.keys())[0] == ('add2', 'mul2')
    assert list(codepack1.links.values())[0] == 'a'


@pytest.mark.parametrize(*test_params)
def test_codepack_iterator(code_class, codepack_class):
    code1 = code_class(add2)
    code2 = code_class(mul2)
    code3 = code_class(add3)
    codepack = codepack_class(codes=[code1])
    code1 > code2
    codes = set()
    for code in codepack:
        codes.add(code.get_id())
    assert codes == {code1.get_id(), code2.get_id()}
    assert code1 in codepack
    assert code2 in codepack
    assert code3 not in codepack


@pytest.mark.parametrize(*test_params)
def test_copying_codepack_by_serialization(code_class, codepack_class):
    code1 = code_class(add2)
    code2 = code_class(mul2)
    code3 = code_class(add3)
    code1 > code2
    code2 >> code3 | 'a'
    codepack1 = codepack_class(codes=[code1])
    codepack2 = codepack_class.from_dict(codepack1.to_dict())
    assert codepack1.to_dict() == codepack2.to_dict()


@pytest.mark.parametrize(*test_params)
def test_giving_code_from_other_codepack(code_class, codepack_class):
    code1 = code_class(add2)
    code2 = code_class(mul2)
    _ = codepack_class(codes=[code1])
    with pytest.raises(ValueError):
        _ = codepack_class(codes=[code2, code1])


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_local_execution_with_subscription(code_class, codepack_class):
    code1 = code_class(add3)
    code2 = code_class(mul2)
    code3 = code_class(combination)
    code4 = code_class(linear)
    code5 = code_class(print_x)
    code3 < [code1, code2]
    code3 > [code4, code5]
    code1 >> code3 | 'c'
    code2 >> code3 | 'd'
    code3 >> code4 | 'c'
    code3 >> code5 | 'x'
    codepack = codepack_class(codes=[code1], subscription=code4.get_id())
    arg1 = Arg('arg1')(1, 2, 3)
    arg2 = Arg('arg2')(a=1, b=2)
    arg3 = Arg('arg3')(a=2, b=5)
    arg4 = Arg('arg4')(a=5, b=7)
    argpack = ArgPack(args=[arg1, arg2, arg3, arg4])
    argpack.map_code(code_id=code1.get_id(), arg_id=arg1.get_id())
    argpack.map_code(code_id=code2.get_id(), arg_id=arg2.get_id())
    argpack.map_code(code_id=code3.get_id(), arg_id=arg3.get_id())
    argpack.map_code(code_id=code4.get_id(), arg_id=arg4.get_id())
    assert await run_function(codepack.__call__, argpack) == 57


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_local_execution_without_subscription(code_class, codepack_class):
    code1 = code_class(add3)
    code2 = code_class(mul2)
    code3 = code_class(combination)
    code4 = code_class(linear)
    code5 = code_class(print_x)
    code3 < [code1, code2]
    code3 > [code4, code5]
    code1 >> code3 | 'c'
    code2 >> code3 | 'd'
    code3 >> code4 | 'c'
    code3 >> code5 | 'x'
    codepack = codepack_class(codes=[code1])
    arg1 = Arg('arg1')(1, 2, 3)
    arg2 = Arg('arg2')(a=1, b=2)
    arg3 = Arg('arg3')(a=2, b=5)
    arg4 = Arg('arg4')(a=5, b=7)
    argpack = ArgPack(args=[arg1, arg2, arg3, arg4])
    argpack.map_code(code_id=code1.get_id(), arg_id=arg1.get_id())
    argpack.map_code(code_id=code2.get_id(), arg_id=arg2.get_id())
    argpack.map_code(code_id=code3.get_id(), arg_id=arg3.get_id())
    argpack.map_code(code_id=code4.get_id(), arg_id=arg4.get_id())
    assert await run_function(codepack.__call__, argpack) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_local_execution_without_dependency(code_class, codepack_class):
    code1 = code_class(add3)
    code2 = code_class(mul2)
    code3 = code_class(combination)
    code4 = code_class(linear)
    code5 = code_class(print_x)
    code3 < [code1, code2]
    code3 > [code4, code5]
    code1 >> code3 | 'c'
    code2 >> code3 | 'd'
    code3 > code4
    code3 >> code5 | 'x'
    codepack = codepack_class(codes=[code1], subscription=code4.get_id())
    arg1 = Arg('arg1')(1, 2, 3)
    arg2 = Arg('arg2')(a=1, b=2)
    arg3 = Arg('arg3')(a=2, b=5)
    arg4 = Arg('arg4')(a=5, b=7, c=2)
    argpack = ArgPack(args=[arg1, arg2, arg3, arg4])
    argpack.map_code(code_id=code1.get_id(), arg_id=arg1.get_id())
    argpack.map_code(code_id=code2.get_id(), arg_id=arg2.get_id())
    argpack.map_code(code_id=code3.get_id(), arg_id=arg3.get_id())
    argpack.map_code(code_id=code4.get_id(), arg_id=arg4.get_id())
    assert await run_function(codepack.__call__, argpack) == 37


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_local_execution_without_argpack(code_class, codepack_class):
    def return_3():
        return 3

    def return_5():
        return 5

    code1 = code_class(return_3)
    code2 = code_class(return_5)
    code3 = code_class(add2)
    code3 < [code1, code2]
    code1 >> code3 | 'a'
    code2 >> code3 | 'b'
    codepack = codepack_class(codes=[code1], subscription=code3.get_id())
    assert await run_function(codepack.__call__) == 8


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_local_execution_with_error(code_class, codepack_class):
    code1 = code_class(add3)
    code2 = code_class(mul2)
    code3 = code_class(raise_error)
    code4 = code_class(linear)
    code5 = code_class(print_x)
    code3 < [code1, code2]
    code3 > [code4, code5]
    code1 > code3
    code2 > code3
    code3 > code4
    code3 > code5
    codepack = codepack_class(codes=[code1], subscription=code4.get_id())
    arg1 = Arg('arg1')(1, 2, 3)
    arg2 = Arg('arg2')(a=1, b=2)
    arg4 = Arg('arg4')(a=5, b=7, c=2)
    arg5 = Arg('arg4')(x=2)
    argpack = ArgPack(args=[arg1, arg2, arg4, arg5])
    argpack.map_code(code_id=code1.get_id(), arg_id=arg1.get_id())
    argpack.map_code(code_id=code2.get_id(), arg_id=arg2.get_id())
    argpack.map_code(code_id=code4.get_id(), arg_id=arg4.get_id())
    argpack.map_code(code_id=code5.get_id(), arg_id=arg5.get_id())
    with pytest.raises(SystemError):
        _ = await run_function(codepack.__call__, argpack)


@mock.patch('codepack.codepack.DoubleKeyMap')
@pytest.mark.parametrize(*test_params)
def test_error_occurred_during_update(mock_class, code_class, codepack_class):
    mock_class().remove_all.side_effect = mock.Mock(side_effect=SystemError())
    code = code_class(add2)
    with pytest.raises(SystemError):
        _ = codepack_class(codes=[code])
