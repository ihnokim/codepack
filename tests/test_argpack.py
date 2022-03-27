from codepack import Code, CodePack, CodePackSnapshot
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
    with pytest.raises(TypeError):
        argpack['mul2'](c=5)
    argpack['mul2'](a=2)
    assert argpack['add2']['a'] == 3
    assert argpack['add2']['b'] == 2
    assert argpack['mul2']['a'] == 2
    assert codepack(argpack=argpack) == 10


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
    with pytest.raises(TypeError):
        argpack['mul2'](c=5)
    argpack['mul2'](a=2)
    assert argpack['add2']['a'] == 3
    assert argpack['add2']['b'] == 2
    assert argpack['mul2']['a'] == 2
    assert argpack.__str__() == 'ArgPack(id: argpack_test, args: {add2(a=3, b=2), mul2(a=2)})'
    assert codepack(argpack=argpack) == 10
