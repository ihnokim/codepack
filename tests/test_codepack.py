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
    argpack['add3']['a'] = 1
    argpack['add3']['b'] = 2
    argpack['add3']['c'] = 3
    argpack['mul2']['a'] = 1
    argpack['mul2']['b'] = 2
    argpack['combination']['a'] = 2
    argpack['combination']['b'] = 5
    argpack['linear']['b'] = 7
    argpack['linear']['a'] = 5
    ret = cp(argpack)
    assert ret == 57
