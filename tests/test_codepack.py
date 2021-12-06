from codepack import Code, CodePack
from tests import *


def test_codepack(default_os_env):
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
    arg_dict = cp.make_arg_dict()
    arg_dict['add3']['a'] = 1
    arg_dict['add3']['b'] = 2
    arg_dict['add3']['c'] = 3
    arg_dict['mul2']['a'] = 1
    arg_dict['mul2']['b'] = 2
    arg_dict['combination']['a'] = 2
    arg_dict['combination']['b'] = 5
    arg_dict['linear']['b'] = 7
    arg_dict['linear']['a'] = 5
    ret = cp(arg_dict)
    assert ret == 57
