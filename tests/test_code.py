from codepack import Code, Callback, StorageService
from codepack.storages import MongoStorage
from tests import *
import pytest
from datetime import datetime
from unittest.mock import MagicMock
from functools import partial


def test_assert_param(default_os_env):
    code1 = Code(add2)
    with pytest.raises(AssertionError):
        code1.assert_param('c')
    code2 = Code(dummy_function1)
    reserved_params = ['a', 'b', 'c', 'd']
    for param in reserved_params:
        code2.assert_param(param)
    with pytest.raises(AssertionError):
        code2.assert_param('e')
    code3 = Code(dummy_function2)
    for param in reserved_params + ['e', 'f']:
        code3.assert_param(param)


def test_get_reserved_params(default_os_env):
    code1 = Code(add3)
    assert dict(code1.get_reserved_params()) == {'a': None, 'b': None, 'c': 2}
    code2 = Code(dummy_function1)
    assert dict(code2.get_reserved_params()) == {'a': None, 'b': 2, 'c': None, 'd': 3}
    code3 = Code(dummy_function2)
    assert dict(code3.get_reserved_params()) == {'a': None, 'b': 2, 'c': None, 'd': 3}


def test_print_params(default_os_env):
    code1 = Code(add3)
    assert code1.print_params() == '(a, b, c=2)'
    code2 = Code(dummy_function1)
    params_old = "(a:dict, b:str=2, *args:'Code', c:Any, d=3) -> int"
    params_new = "(a: dict, b: str = 2, *args: 'Code', c: Any, d=3) -> int"
    assert code2.print_params() == params_old or code2.print_params() == params_new
    params_old = "(a:dict, b:str=2, *args:'Code', c:Any, d=3, **kwargs:list) -> None"
    params_new = "(a: dict, b: str = 2, *args: 'Code', c: Any, d=3, **kwargs: list) -> None"
    code3 = Code(dummy_function2)
    assert code3.print_params() == params_old or code3.print_params() == params_new


def test_load_code_with_annotations(default_os_env, fake_mongodb):
    mongo_storage = MongoStorage(mongodb=fake_mongodb, db='test_db', collection='test_collection',
                                 item_type=Code, key='id')
    storage_service = StorageService(storage=mongo_storage)
    Code(add3, storage_service=storage_service).save()
    code1 = Code.load('add3', storage_service=storage_service)
    assert code1.print_params() == '(a, b, c=2)'
    Code(dummy_function1, storage_service=storage_service).save()
    code2 = Code.load('dummy_function1', storage_service=storage_service)
    params_old = "(a:dict, b:str=2, *args:'Code', c:Any, d=3) -> int"
    params_new = "(a: dict, b: str = 2, *args: 'Code', c: Any, d=3) -> int"
    assert code2.print_params() == params_old or code2.print_params() == params_new
    params_old = "(a:dict, b:str=2, *args:'Code', c:Any, d=3, **kwargs:list) -> None"
    params_new = "(a: dict, b: str = 2, *args: 'Code', c: Any, d=3, **kwargs: list) -> None"
    Code(dummy_function2, storage_service=storage_service).save()
    code3 = Code.load('dummy_function2', storage_service=storage_service)
    assert code3.print_params() == params_old or code3.print_params() == params_new


def test_print_info(default_os_env):
    code1 = Code(add2, env='test-env')
    code2 = Code(add3, image='test-image', owner='admin')
    code1 >> code2
    code2.receive('b') << code1
    assert code1.get_info() == "Code(id: add2, function: add2, params: (a, b), receive: {}," \
                               " env: test-env, state: UNKNOWN)"
    assert code2.get_info() == "Code(id: add3, function: add3, params: (a, b, c=2), receive: {'b': 'add2'}," \
                               " image: test-image, owner: admin, state: UNKNOWN)"
    assert code1.get_info(state=False) == "Code(id: add2, function: add2, params: (a, b), receive: {}, env: test-env)"
    assert code2.get_info(state=False) == "Code(id: add3, function: add3, params: (a, b, c=2), receive: {'b': 'add2'}," \
                                          " image: test-image, owner: admin)"
    code1.image = 'test-image2'
    code1.owner = 'admin2'
    assert code1.get_info() == "Code(id: add2, function: add2, params: (a, b), receive: {}," \
                               " env: test-env, image: test-image2, owner: admin2, state: UNKNOWN)"
    assert code1.get_info(state=False) == "Code(id: add2, function: add2, params: (a, b), receive: {}," \
                                          " env: test-env, image: test-image2, owner: admin2)"
    code2.owner = None
    assert code2.get_info() == "Code(id: add3, function: add3, params: (a, b, c=2), receive: {'b': 'add2'}," \
                               " image: test-image, state: UNKNOWN)"
    assert code2.get_info(state=False) == "Code(id: add3, function: add3, params: (a, b, c=2), receive: {'b': 'add2'}," \
                                          " image: test-image)"
    code2.image = None
    assert code2.get_info() == "Code(id: add3, function: add3, params: (a, b, c=2), receive: {'b': 'add2'}," \
                               " state: UNKNOWN)"
    assert code2.get_info(
        state=False) == "Code(id: add3, function: add3, params: (a, b, c=2), receive: {'b': 'add2'})"


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
    dependency = [{'id': 'test1', 'serial_number': '1234', 'param': None},
                  {'id': 'test2', 'serial_number': '5678', 'param': 'a'}]
    code = Code(add2, dependency=dependency)
    assert '1234' in code.dependency and '5678' in code.dependency
    assert code.dependency['5678'].param == 'a'
    assert not code.dependency['1234'].param
    tmp = code.dependency.get_params()
    assert len(tmp) == 1 and 'a' in tmp


def test_check_dependency_linkage(default_os_env):
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
    assert not code4.dependency[code3.serial_number].param
    code4.receive('c') << code3
    assert code4.dependency[code3.serial_number].param == 'c'
    code3.receive('b') << code1
    assert code3.dependency[code1.serial_number].param == 'b'
    assert not code2.dependency[code1.serial_number].param
    code3 // code4
    assert len(set(code4.dependency)) == 0
    assert code3.id not in code4.parents
    assert code4.id not in code3.children
    code1 // [code2, code3]
    assert len(code2.dependency.keys()) == len(code3.dependency.keys()) == 0
    assert len(code1.children) == 0
    assert len(code2.parents) == 0 and len(code3.parents) == 0


def test_check_dependency_state(default_os_env):
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


def test_dependency_error_propagation(default_os_env):
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    code1 >> code3
    code2 >> code3
    assert code3.get_state() == 'UNKNOWN'
    with pytest.raises(TypeError):
        code1()
    assert code1.get_state() == 'ERROR'
    ret = code3(1, 2, 3)
    assert ret is None
    assert code3.get_state() == 'WAITING'


def test_validate_dependency_result(default_os_env):
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    code1 >> code3
    code2 >> code3
    code3.receive('b') << code1
    code3.receive('c') << code2
    code1(1, 2)
    code2(3, 4)
    code2.service['delivery'].cancel(code2.serial_number)
    ret = code3(a=3)
    assert ret is None
    assert code3.get_state() == 'ERROR'
    code2(3, 4)
    code2.send_result(item=1, timestamp=datetime.now().timestamp() + 1)
    ret = code3(a=3)
    assert ret is 7
    assert code3.get_state() == 'TERMINATED'
    code2(3, 4)
    snapshots = code3.dependency.load_snapshot()
    assert len(snapshots) == 2
    assert code3.dependency.check_delivery() is True
    snapshot_dict = {x['_id']: x for x in snapshots}
    code2_state_info = snapshot_dict.pop(code2.serial_number)
    assert code3.dependency.validate(snapshot=snapshot_dict.values()) == 'WAITING'
    snapshot_dict[code2.serial_number] = code2_state_info
    assert code3.dependency.validate(snapshot=snapshot_dict.values()) == 'READY'
    code2.service['delivery'].cancel(code2.serial_number)
    assert code3.dependency.validate(snapshot=snapshot_dict.values()) == 'ERROR'
    code2.service['delivery'].send(id='dummy', serial_number=code2.serial_number, item=123)
    assert code3.dependency.validate(snapshot=snapshot_dict.values()) == 'READY'


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


def test_load_code_from_storage_service_with_id(default_os_env):
    code1 = Code(add2)
    code1.save()
    code2 = Code(id='add2', serial_number='1234')
    assert code1.source == code2.source
    assert code1(1, 2) == code2(1, 2)
    assert code1.serial_number != code2.serial_number
    assert code2.serial_number == '1234'


def test_update_serial_number(default_os_env):
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    code4 = Code(linear)
    code1 >> code2
    code2 >> [code3, code4]
    code2.receive('a') << code1
    code3.receive('b') << code2
    code4.receive('c') << code2
    old_serial_number = code2.serial_number
    new_serial_number = '1234'
    code2.update_serial_number(new_serial_number)
    assert code2.serial_number == new_serial_number
    assert code2.id in code1.children
    assert code1.children[code2.id].serial_number == new_serial_number
    assert code2.id in code3.parents
    assert code3.parents[code2.id].serial_number == new_serial_number
    assert old_serial_number not in code3.dependency
    assert new_serial_number in code3.dependency
    assert code3.dependency[new_serial_number].param == 'b'
    assert code3.dependency[new_serial_number].code == code3
    assert code3.dependency[new_serial_number].id == code2.id
    assert code3.dependency[new_serial_number].serial_number == new_serial_number
    assert code2.id in code4.parents
    assert code4.parents[code2.id].serial_number == new_serial_number
    assert old_serial_number not in code4.dependency
    assert new_serial_number in code4.dependency
    assert code4.dependency[new_serial_number].param == 'c'
    assert code4.dependency[new_serial_number].code == code4
    assert code4.dependency[new_serial_number].id == code2.id
    assert code4.dependency[new_serial_number].serial_number == new_serial_number


def test_collect_linked_ids(default_os_env):
    code1 = Code(add2)
    code2 = Code(add3)
    code3 = Code(mul2)
    code4 = Code(print_x)
    code5 = Code(linear)
    code1 >> code2 >> code3
    code2 >> code4
    code5 >> code4
    all_ids = {'add2', 'add3', 'mul2', 'print_x', 'linear'}
    assert code1._collect_linked_ids() == all_ids
    assert code2._collect_linked_ids() == all_ids
    assert code3._collect_linked_ids() == all_ids
    assert code4._collect_linked_ids() == all_ids


def test_recursion_detection(default_os_env):
    code1 = Code(add2)
    code2 = Code(add3)
    code3 = Code(mul2)
    code4 = Code(print_x)
    code1 >> code2
    all_ids = {'add2', 'add3'}
    assert code1._collect_linked_ids() == all_ids
    assert code2._collect_linked_ids() == all_ids
    code2 >> code3
    all_ids.add('mul2')
    assert code1._collect_linked_ids() == all_ids
    assert code2._collect_linked_ids() == all_ids
    assert code3._collect_linked_ids() == all_ids
    with pytest.raises(ValueError):
        code3 >> code2
    code3 >> code4
    all_ids.add('print_x')
    assert code1._collect_linked_ids() == all_ids
    assert code2._collect_linked_ids() == all_ids
    assert code3._collect_linked_ids() == all_ids
    assert code4._collect_linked_ids() == all_ids


def test_default_load(default_os_env):
    code1 = Code(add2)
    code2 = Code(add3)
    search_result = Code.load(['add2', 'add3'])
    assert type(search_result) == list and len(search_result) == 0
    code2.save()
    search_result = Code.load(['add2', 'add3'])
    assert type(search_result) == list and len(search_result) == 1
    assert isinstance(search_result[0], Code) and search_result[0].id == 'add3'
    search_result = Code.load('add2')
    assert search_result is None
    search_result = Code.load('add3')
    assert search_result is not None
    assert isinstance(search_result, Code) and search_result.id == 'add3'


def test_get_message(default_os_env):
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(combination)
    result1 = None
    with pytest.raises(TypeError):
        result1 = code1(a=2)
    assert result1 is None
    assert code1.get_state() == 'ERROR'
    assert code1.get_message() == "add2() missing 1 required positional argument: 'b'"
    result2 = code2(a=2, b=3)
    assert result2 == 6
    assert code2.get_state() == 'TERMINATED'
    assert code2.get_message() == ''
    assert code3.get_state() == 'UNKNOWN'
    assert code3.get_message() == ''


def test_create_code_without_anything(default_os_env):
    with pytest.raises(AssertionError):
        Code()


def test_remove(default_os_env):
    code = Code(add2)
    ret = Code.load('add2')
    assert ret is None
    code.save()
    ret = Code.load('add2')
    assert ret is not None
    assert isinstance(ret, Code)
    assert ret.id == code.id
    Code.remove('add2')
    ret = Code.load('add2')
    assert ret is None


def test_register_callback_with_names(default_os_env):
    code = Code(add2)
    code.register_callback(callback=dummy_callback1, name=['callback1', 'callback2', 'callback3'])
    assert code.callbacks == {'callback1': dummy_callback1}


def test_register_multiple_callbacks_without_names(default_os_env):
    code = Code(add2)
    code.register_callback(callback=[dummy_callback1, partial(dummy_callback2, 'test'), Callback(dummy_callback3)])
    assert len(code.callbacks) == 3
    assert set(code.callbacks.keys()) == {'dummy_callback1', 'dummy_callback2', 'dummy_callback3'}


def test_register_multiple_callbacks_with_one_name(default_os_env):
    code = Code(add2)
    with pytest.raises(IndexError):
        code.register_callback(callback=[dummy_callback1, partial(dummy_callback2, 'test'), Callback(dummy_callback3)],
                               name='dummy_callback')
    assert len(code.callbacks) == 0
    code.register_callback(callback=[dummy_callback1], name='dummy_callback')
    assert len(code.callbacks) == 1
    assert 'dummy_callback' in code.callbacks
    assert code.callbacks['dummy_callback'] == dummy_callback1


def test_register_multiple_callbacks_with_multiple_names(default_os_env):
    code = Code(add2)
    _dummy_callback2 = partial(dummy_callback2, 'test')
    _dummy_callback3 = Callback(dummy_callback3)
    with pytest.raises(IndexError):
        code.register_callback(callback=[dummy_callback1, _dummy_callback2, _dummy_callback3],
                               name=['dummy_callback'])
    assert len(code.callbacks) == 0
    code.register_callback(callback=[dummy_callback1, _dummy_callback2, _dummy_callback3],
                           name=['callback1', 'callback2', 'callback3'])
    assert len(code.callbacks) == 3
    assert code.callbacks == {'callback1': dummy_callback1,
                              'callback2': _dummy_callback2,
                              'callback3': _dummy_callback3}


def test_run_callback_with_normal_case(default_os_env):
    mock_function = MagicMock()
    code = Code(add2)
    code.register_callback(callback=mock_function, name='test_function')
    mock_function.assert_not_called()
    code(a=3, b=5)
    arg_list = mock_function.call_args_list
    assert len(arg_list) == 3
    seq = ['READY', 'RUNNING', 'TERMINATED']
    for i, (args, kwargs) in enumerate(arg_list):
        assert len(args) == 1 and len(kwargs) == 0
        assert args[0] == {'serial_number': code.serial_number, 'state': seq[i]}


def test_run_callback_with_error_case(default_os_env):
    mock_function = MagicMock()
    code = Code(add2)
    code.register_callback(callback=mock_function, name='test_function')
    mock_function.assert_not_called()
    with pytest.raises(TypeError):
        code(a=3)
    arg_list = mock_function.call_args_list
    assert len(arg_list) == 3
    seq = ['READY', 'RUNNING', 'ERROR']
    for i, (args, kwargs) in enumerate(arg_list):
        assert len(args) == 1 and len(kwargs) == 0
        x = {'serial_number': code.serial_number, 'state': seq[i]}
        if seq[i] == 'ERROR':
            x['message'] = "add2() missing 1 required positional argument: 'b'"
        assert args[0] == x


def test_run_multiple_callbacks_with_normal_case(default_os_env):
    mock_function1 = MagicMock()
    mock_function2 = MagicMock()
    code = Code(add2)
    code.register_callback(callback=[mock_function1, mock_function2], name=['test_function1', 'test_function2'])
    mock_function1.assert_not_called()
    mock_function2.assert_not_called()
    code(a=3, b=5)
    for mock_function in [mock_function1, mock_function2]:
        arg_list = mock_function.call_args_list
        assert len(arg_list) == 3
        seq = ['READY', 'RUNNING', 'TERMINATED']
        for i, (args, kwargs) in enumerate(arg_list):
            assert len(args) == 1 and len(kwargs) == 0
            assert args[0] == {'serial_number': code.serial_number, 'state': seq[i]}


def test_run_multiple_callbacks_with_error_case(default_os_env):
    mock_function1 = MagicMock()
    mock_function2 = MagicMock()
    code = Code(add2)
    code.register_callback(callback=[mock_function1, mock_function2], name=['test_function1', 'test_function2'])
    mock_function1.assert_not_called()
    mock_function2.assert_not_called()
    with pytest.raises(TypeError):
        code(a=3)
    for mock_function in [mock_function1, mock_function2]:
        arg_list = mock_function.call_args_list
        assert len(arg_list) == 3
        seq = ['READY', 'RUNNING', 'ERROR']
        for i, (args, kwargs) in enumerate(arg_list):
            assert len(args) == 1 and len(kwargs) == 0
            x = {'serial_number': code.serial_number, 'state': seq[i]}
            if seq[i] == 'ERROR':
                x['message'] = "add2() missing 1 required positional argument: 'b'"
            assert args[0] == x
