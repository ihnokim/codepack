from codepack import Code, CodePack
from tests import *
import pytest


def test_sync_codepack(default_os_env):
    c1 = Code(add3)
    c2 = Code(mul2)
    c3 = Code(combination)
    c4 = Code(linear)
    c5 = Code(print_x)
    c1 >> c3
    c2 >> c3
    c3 >> [c4, c5]
    c3.receive('c') << c1
    c3.receive('d') << c2
    c4.receive('c') << c3
    c5.receive('x') << c3
    codepack = CodePack(id='test_codepack', code=c1, subscribe=c4)
    assert codepack.get_state() == 'UNKNOWN'
    argpack = codepack.make_argpack()
    argpack['add3'](a=1, b=2, c=3)
    argpack['mul2'](a=1, b=2)
    argpack['combination'](a=2, b=5)
    argpack['linear'](a=5, b=7)
    result = codepack(argpack)
    assert codepack.get_state() == 'TERMINATED'
    assert result == 57


def test_default_load(default_os_env):
    code1 = Code(add2)
    code2 = Code(add3)
    code1 >> code2
    code2.receive('b') << code1
    codepack = CodePack(id='test_codepack', code=code1, subscribe=code2)
    search_result = CodePack.load(['test_codepack', 'dummy'])
    assert type(search_result) == list and len(search_result) == 0
    codepack.save()
    search_result = CodePack.load(['test_codepack', 'dummy'])
    assert type(search_result) == list and len(search_result) == 1
    assert isinstance(search_result[0], CodePack) and search_result[0].id == 'test_codepack'
    search_result = CodePack.load('dummy')
    assert search_result is None
    search_result = CodePack.load('test_codepack')
    assert search_result is not None
    assert isinstance(search_result, CodePack) and search_result.id == 'test_codepack'
    assert search_result.root._collect_linked_ids() == codepack.root._collect_linked_ids()
    argpack = codepack.make_argpack()
    argpack['add2'](a=2, b=5)
    argpack['add3'](a=3, c=2)
    assert search_result(argpack=argpack) == codepack(argpack=argpack)


def test_save_and_load_argpack(default_os_env):
    code1 = Code(add2)
    code2 = Code(add3)
    code1 >> code2
    code2.receive('b') << code1
    codepack = CodePack(id='test_codepack', code=code1)
    argpack = codepack.make_argpack()
    argpack['add2'](a=2, b=5)
    argpack['add3'](a=3, c=2)
    codepack.save_argpack(argpack)
    argpack2 = codepack.load_argpack(id=codepack.id)
    assert argpack['add2']['a'] == argpack2['add2']['a']
    assert argpack['add2']['b'] == argpack2['add2']['b']
    assert argpack['add3']['a'] == argpack2['add3']['a']
    assert argpack['add3']['c'] == argpack2['add3']['c']


def test_get_str(default_os_env):
    c1 = Code(add3)
    c2 = Code(mul2, owner='codepack', env='codepack-env')
    c3 = Code(combination, image='my-image:0.0.1')
    c4 = Code(linear, owner='codepack')
    c5 = Code(print_x)
    c1 >> c3
    c2 >> c3
    c3 >> [c4, c5]
    c3.receive('c') << c1
    c3.receive('d') << c2
    c4.receive('c') << c3
    c5.receive('x') << c3
    cp = CodePack(id='test_codepack', code=c1, subscribe=c4, owner='codepack')
    expected_str1 = "CodePack(id: test_codepack, subscribe: linear, owner: codepack)\n" \
                    "| Code(id: add3, function: add3, params: (a, b, c=2), receive: {})\n" \
                    "|- Code(id: combination, function: combination, params: (a, b, c, d), " \
                    "receive: {'c': 'add3', 'd': 'mul2'}, image: my-image:0.0.1)\n" \
                    "|-- Code(id: print_x, function: print_x, params: (x), receive: {'x': 'combination'})\n" \
                    "|-- Code(id: linear, function: linear, params: (a, b, c), " \
                    "receive: {'c': 'combination'}, owner: codepack)\n" \
                    "| Code(id: mul2, function: mul2, params: (a, b), " \
                    "receive: {}, env: codepack-env, owner: codepack)\n" \
                    "|- Code(id: combination, function: combination, params: (a, b, c, d), " \
                    "receive: {'c': 'add3', 'd': 'mul2'}, image: my-image:0.0.1)\n" \
                    "|-- Code(id: print_x, function: print_x, params: (x), receive: {'x': 'combination'})\n" \
                    "|-- Code(id: linear, function: linear, params: (a, b, c), " \
                    "receive: {'c': 'combination'}, owner: codepack)"
    expected_str2 = "CodePack(id: test_codepack, subscribe: linear, owner: codepack)\n" \
                    "| Code(id: mul2, function: mul2, params: (a, b), " \
                    "receive: {}, env: codepack-env, owner: codepack)\n" \
                    "|- Code(id: combination, function: combination, params: (a, b, c, d), " \
                    "receive: {'c': 'add3', 'd': 'mul2'}, image: my-image:0.0.1)\n" \
                    "|-- Code(id: print_x, function: print_x, params: (x), receive: {'x': 'combination'})\n" \
                    "|-- Code(id: linear, function: linear, params: (a, b, c), " \
                    "receive: {'c': 'combination'}, owner: codepack)\n" \
                    "| Code(id: add3, function: add3, params: (a, b, c=2), receive: {})\n" \
                    "|- Code(id: combination, function: combination, params: (a, b, c, d), " \
                    "receive: {'c': 'add3', 'd': 'mul2'}, image: my-image:0.0.1)\n" \
                    "|-- Code(id: print_x, function: print_x, params: (x), receive: {'x': 'combination'})\n" \
                    "|-- Code(id: linear, function: linear, params: (a, b, c), " \
                    "receive: {'c': 'combination'}, owner: codepack)"
    assert cp.__str__() == expected_str1 or cp.__str__() == expected_str2
    assert cp.__repr__() == expected_str1 or cp.__repr__() == expected_str2


def test_remove(default_os_env):
    code1 = Code(add2)
    code2 = Code(mul2)
    code1 >> code2
    codepack = CodePack(id='test_codepack', code=code1, subscribe=code2)
    ret = CodePack.load('test_codepack')
    assert ret is None
    codepack.save()
    ret = CodePack.load('test_codepack')
    assert ret is not None
    assert isinstance(ret, CodePack)
    assert ret.id == codepack.id
    CodePack.remove('test_codepack')
    ret = CodePack.load('test_codepack')
    assert ret is None


def test_get_message(default_os_env):
    c1 = Code(add3)
    c2 = Code(mul2, owner='codepack', env='codepack-env')
    c3 = Code(combination, image='my-image:0.0.1')
    c4 = Code(linear, owner='codepack')
    c5 = Code(print_x)
    c1 >> c3
    c2 >> c3
    c3 >> [c4, c5]
    c3.receive('c') << c1
    c3.receive('d') << c2
    c4.receive('c') << c3
    c5.receive('x') << c3
    codepack = CodePack(id='test_codepack', code=c1, subscribe=c4, owner='codepack')
    argpack = codepack.make_argpack()
    argpack['add3'](a=1, b=2, c=3)
    argpack['mul2'](a=1, b=2)
    argpack['combination'](a=2)
    argpack['linear'](a=5, b=7)
    with pytest.raises(TypeError):
        codepack(argpack)
    assert codepack.get_state() == 'ERROR'
    assert c3.get_state() == 'ERROR'
    messages = codepack.get_message()
    assert len(messages) == 1 and 'combination' in messages
    assert messages['combination'] == c3.get_message() == "unsupported operand type(s) for *: 'int' and 'NoneType'"
    assert codepack.get_result() is None
