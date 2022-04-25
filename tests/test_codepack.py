from codepack import Code, CodePack
from tests import *


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

    cp = CodePack(id='test_codepack', code=c1, subscribe=c4)
    argpack = cp.make_argpack()
    argpack['add3'](a=1, b=2, c=3)
    argpack['mul2'](a=1, b=2)
    argpack['combination'](a=2, b=5)
    argpack['linear'](a=5, b=7)
    ret = cp(argpack)
    assert ret == 57


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
