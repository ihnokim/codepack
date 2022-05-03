from codepack import Code, CodePack, ArgPack, CodePackSnapshot
from tests import *
import pytest


def test_argpack_input(default_os_env):
    code1 = Code(add2)
    code2 = Code(mul2)
    code1 >> code2
    code2.receive('b') << code1
    codepack = CodePack(id='argpack_test', code=code1, subscribe=code2)
    argpack = codepack.make_argpack()
    argpack['add2'](a=3, b=2)
    argpack['mul2'](c=5)
    argpack['mul2'](a=2)
    assert argpack['add2']['a'] == 3
    assert argpack['add2']['b'] == 2
    assert argpack['mul2']['a'] == 2
    with pytest.raises(TypeError):
        codepack(argpack=argpack)
    assert codepack.get_state() == 'ERROR'
    assert codepack.get_message() == {'mul2': "mul2() got an unexpected keyword argument 'c'"}
    argpack['mul2'].kwargs.pop('c')
    result = codepack(argpack=argpack)
    assert codepack.get_state() == 'TERMINATED'
    assert result == 10


def test_argpack_codepack_execution(default_os_env):
    code1 = Code(add2)
    code2 = Code(mul2)
    code1 >> code2
    code2.receive('b') << code1
    codepack = CodePack(id='argpack_test', code=code1, subscribe=code2)
    argpack = codepack.make_argpack()
    snapshot = codepack.to_snapshot(argpack=argpack)
    assert isinstance(snapshot.argpack, dict)
    assert snapshot.argpack['add2']['a'] is None
    assert snapshot.argpack['add2']['b'] is None
    assert snapshot.argpack['mul2']['a'] is None
    assert 'b' not in snapshot.argpack['mul2']
    snapshot2 = CodePackSnapshot.from_dict(snapshot.to_dict())
    assert isinstance(snapshot2.argpack, dict)
    assert snapshot2.argpack['add2']['a'] is None
    assert snapshot2.argpack['add2']['b'] is None
    assert snapshot2.argpack['mul2']['a'] is None
    assert 'b' not in snapshot2.argpack['mul2']
    argpack['add2'](a=3, b=2)
    argpack['mul2'](a=2)
    snapshot3 = codepack.to_snapshot(argpack=argpack)
    assert snapshot3.argpack['add2']['a'] == 3
    assert snapshot3.argpack['add2']['b'] == 2
    assert snapshot3.argpack['mul2']['a'] == 2
    snapshot4 = CodePackSnapshot.from_dict(snapshot3.to_dict())
    assert snapshot4.argpack['add2']['a'] == 3
    assert snapshot4.argpack['add2']['b'] == 2
    assert snapshot4.argpack['mul2']['a'] == 2
    codepack2 = CodePack.from_snapshot(snapshot4)
    assert codepack2(argpack=snapshot4.argpack) == 10


def test_argpack_str(default_os_env):
    code1 = Code(add2)
    code2 = Code(mul2)
    code1 >> code2
    code2.receive('b') << code1
    codepack = CodePack(id='argpack_test', code=code1, subscribe=code2)
    argpack = codepack.make_argpack()
    assert argpack.__str__() == 'ArgPack(id: argpack_test, args: {add2(a=None, b=None), mul2(a=None)})'
    argpack['add2'](a=3, b=2)
    argpack['mul2'](c=5)
    argpack['mul2'](a=2)
    assert argpack['add2']['a'] == 3
    assert argpack['add2']['b'] == 2
    assert argpack['mul2']['a'] == 2
    assert argpack.__str__() == 'ArgPack(id: argpack_test, args: {add2(a=3, b=2), mul2(a=2, c=5)})'
    with pytest.raises(TypeError):
        codepack(argpack=argpack)
    assert codepack.get_state() == 'ERROR'
    assert codepack.get_message() == {'mul2': "mul2() got an unexpected keyword argument 'c'"}
    argpack['mul2'].kwargs.pop('c')
    result = codepack(argpack=argpack)
    assert codepack.get_state() == 'TERMINATED'
    assert result == 10


def test_default_load(default_os_env):
    code1 = Code(add2)
    code2 = Code(add3)
    code1 >> code2
    code2.receive('b') << code1
    codepack = CodePack(id='test_codepack', code=code1, subscribe=code2)
    argpack = codepack.make_argpack()
    argpack['add2'](a=2, b=5)
    argpack['add3'](a=3, c=2)
    search_result = ArgPack.load(['test_codepack', 'dummy'])
    assert type(search_result) == list and len(search_result) == 0
    argpack.save()
    search_result = ArgPack.load(['test_codepack', 'dummy'])
    assert type(search_result) == list and len(search_result) == 1
    assert isinstance(search_result[0], ArgPack) and search_result[0].id == 'test_codepack'
    search_result = ArgPack.load('dummy')
    assert search_result is None
    search_result = ArgPack.load('test_codepack')
    assert search_result is not None
    assert isinstance(search_result, ArgPack) and search_result.id == 'test_codepack'
    assert search_result['add2']['a'] == 2 and search_result['add2']['b'] == 5
    assert search_result['add3']['a'] == 3 and search_result['add3']['c'] == 2


def test_remove(default_os_env):
    code1 = Code(add2)
    code2 = Code(mul2)
    code1 >> code2
    codepack = CodePack(id='test_codepack', code=code1, subscribe=code2)
    argpack = codepack.make_argpack()
    ret = ArgPack.load('test_codepack')
    assert ret is None
    argpack.save()
    ret = ArgPack.load('test_codepack')
    assert ret is not None
    assert isinstance(ret, ArgPack)
    assert ret.id == argpack.id
    ArgPack.remove('test_codepack')
    ret = ArgPack.load('test_codepack')
    assert ret is None
