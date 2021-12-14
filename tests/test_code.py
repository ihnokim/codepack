from codepack import Code
from tests import *
import pytest


def test_assert_arg(default_os_env):
    code1 = Code(add2)
    with pytest.raises(AssertionError):
        code1.assert_arg('c')


def test_get_function_from_source(default_os_env):
    source = """def plus1(x):\n  return x + 1"""
    code = Code(source=source)
    ret = code(x=5)
    assert ret == 6
    assert code.get_state() == 'TERMINATED'


def test_from_dict(default_os_env):
    d = {'_id': 'test', 'source': "def plus1(x):\n  return x + 1"}
    code = Code.from_dict(d)
    assert code.id == 'test'
    assert code.function.__name__ == 'plus1'
    ret = code(x=5)
    assert ret == 6
    assert code.get_state() == 'TERMINATED'


def test_to_dict(default_os_env):
    code = Code(add2)
    d = code.to_dict()
    assert code.id == d['_id'] and code.source == d['source'] and code.description == "exec a + b = ?"


def test_to_db_and_from_db(default_os_env, fake_mongodb):
    db = 'test'
    collection = 'codes'
    test_id = 'add2'
    try:
        code1 = Code(add2)
        assert test_id == code1.id
        code1.to_db(mongodb=fake_mongodb, db=db, collection=collection)
        assert fake_mongodb[db][collection].find_one({'_id': code1.id})
        code2 = Code.from_db(id=code1.id, mongodb=fake_mongodb, db=db, collection=collection)
        assert code1.id == code2.id
        assert code1.function.__name__ == code2.function.__name__
        assert code1.description == code2.description
        assert code1.serial_number != code2.serial_number
        assert code1(1, 3) == code2(1, 3)
    finally:
        if fake_mongodb[db][collection].count_documents({'_id': test_id}) > 0:
            fake_mongodb[db][collection].delete_one({'_id': test_id})


def test_add_dependency(default_os_env):
    dependency = [{'id': 'test1', 'serial_number': '1234', 'arg': None}, {'id': 'test2', 'serial_number': '5678', 'arg': 'a'}]
    code = Code(add2, dependency=dependency)
    assert '1234' in code.dependency and '5678' in code.dependency
    assert code.dependency['5678'].arg == 'a'
    assert not code.dependency['1234'].arg
    tmp = code.get_dependent_args()
    assert len(tmp) == 1 and 'a' in tmp


def test_check_dependency1(default_os_env):
    code1 = Code(add2)
    code2 = Code(print_x)
    code3 = Code(add3)
    code4 = Code(linear)
    code1 >> [code2, code3]
    code3 >> code4
    assert code3.serial_number in code4.dependency
    assert code3.id in code4.parents
    assert code4.id in code3.children
    assert set(code2.dependency.keys()) == set(code3.dependency.keys())
    assert not code4.dependency[code3.serial_number].arg
    code4.receive('c') << code3
    assert code4.dependency[code3.serial_number].arg == 'c'
    code3.receive('b') << code1
    assert code3.dependency[code1.serial_number].arg == 'b'
    assert not code2.dependency[code1.serial_number].arg
    code3 // code4
    assert len(set(code4.dependency)) == 0
    assert code3.id not in code4.parents
    assert code4.id not in code3.children
    code1 // [code2, code3]
    assert len(code2.dependency.keys()) == len(code3.dependency.keys()) == 0
    assert len(code1.children) == 0
    assert len(code2.parents) == 0 and len(code3.parents) == 0


def test_check_dependency2(default_os_env):
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    code1 >> code3
    code2 >> code3
    assert len(code3.dependency) == 2
    ret = code3()
    assert not ret and code3.get_state() == 'WAITING'
    a = code1(a=3, b=5)
    assert a == 8 and code1.get_state() == 'TERMINATED'
    ret = code3()
    assert not ret and code3.get_state() == 'WAITING'
    b = code2(a=2, b=3)
    assert b == 6 and code2.get_state() == 'TERMINATED'
    try:
        ret = code3()
    except TypeError:
        pass
    assert not ret and code3.get_state() == 'ERROR'
    ret = code3(a=1, b=5)
    assert ret == 8 and code3.get_state() == 'TERMINATED'
    code3.receive('a') << code1
    code3.receive('b') << code2
    ret = code3()
    assert ret == 16 and code3.get_state() == 'TERMINATED'
    ret = code3(a=1, b=5)
    assert ret == 8 and code3.get_state() == 'TERMINATED'


def test_default_arg(default_os_env):
    code1 = Code(add2)
    code2 = Code(add3)
    ret = code2(a=1, b=3)
    assert ret == 6 and code2.get_state() == 'TERMINATED'
    code1 >> code2
    code2.receive('c') << code1
    ret = code2(a=1, b=3)
    assert not ret and code2.get_state() == 'WAITING'
    c = code1(a=3, b=5)
    assert c == 8 and code1.get_state() == 'TERMINATED'
    ret = code2(a=1, b=3)
    assert ret == 12 and code2.get_state() == 'TERMINATED'
    code1 // code2
    ret = code2(a=1, b=3)
    assert ret == 6 and code2.get_state() == 'TERMINATED'
