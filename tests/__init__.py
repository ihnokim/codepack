from typing import Any, Optional, Callable
from codepack.code import Code
from codepack.codepack import CodePack
import asyncio
import time


test_config_path = 'tests/config/test.ini'
default_config_path = 'tests/config/default.ini'


def add2(a, b):
    """exec a + b = ?"""
    print('exec add2')
    ret = a + b
    print('add2: %s + %s = %s' % (a, b, ret))
    return ret


async def async_add2(a, b):
    """exec async a + b = ?"""
    print('exec async_add2')
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


def forward(x):
    return x


def do_nothing():
    pass


def dummy_function(a: dict, b: str = 2, c=None, *args: Any, d: Any, e=3, f: Optional[str] = None, **kwargs: 'Code') -> None:
    return None


def raise_error():
    raise SystemError()


def get_codepack_from_code(code: Code) -> Optional[CodePack]:
    if len(code.observable.observers) == 1:
        return list(code.observable.observers.values())[0]
    else:
        return None


async def run_function(function: Callable, *args: Any, **kwargs: Any) -> Any:
    if asyncio.iscoroutinefunction(function):
        return await function(*args, **kwargs)
    else:
        return function(*args, **kwargs)


async def sleep(key: str, seconds: float = 1.0) -> None:
    if 'async' not in key:
        time.sleep(seconds)
    else:
        await asyncio.sleep(seconds)
