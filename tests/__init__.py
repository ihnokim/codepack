from typing import Any
from codepack.code import Code


def add2(a, b):
    """exec a + b = ?"""
    print('exec add2')
    ret = a + b
    print('add2: %s + %s = %s' % (a, b, ret))
    return ret


def mul2(a, b):
    """exec a * b = ?"""
    print('exec mul2')
    ret = a * b
    print('mul2: %s * %s = %s' % (a, b, ret))
    return ret


def add3(a, b, c=2):
    """exec a + b + c = ?"""
    print('exec add3')
    ret = a + b + c
    print('add3: %s + %s + %s = %s' % (a, b, c, ret))
    return ret


def combination(a, b, c, d):
    """exec a * b + c * d"""
    print('exec combination(%s, %s, %s, %s)' % (a, b, c, d))
    ret = a * b + c * d
    print('combination: %s * %s + %s * %s = %s' % (a, b, c, d, ret))
    return ret


def linear(a, b, c):
    """exec a * b + c"""
    print('exec linear')
    ret = a * b + c
    print('linear: %s * %s + %s = %s' % (a, b, c, ret))
    return ret


def print_x(x):
    """exec print_x(x)"""
    print('print_x: %s' % x)


def hello(name):
    """exec hello(name)"""
    ret = 'Hello, %s!' % name
    print(ret)
    return ret


def dummy_callback1(x):
    pass


def dummy_callback2(x1, x2):
    pass


def dummy_callback3(x):
    pass


def dummy_function1(a: dict, b: str = 2, *args: 'Code', c: Any, d=3) -> int:
    return 1


def dummy_function2(a: dict, b: str = 2, *args: 'Code', c: Any, d=3, **kwargs: list) -> None:
    return None
