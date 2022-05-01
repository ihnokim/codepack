from codepack import CodePack, Snapshot, CodeSnapshot
from tests import *
from datetime import datetime
import pytest


def test_snapshot_to_dict_and_from_dict():
    snapshot1 = Snapshot(id='1234', serial_number='5678', custom_value=9)
    snapshot_dict1 = snapshot1.to_dict()
    snapshot2 = Snapshot.from_dict(snapshot_dict1)
    assert snapshot1.id == snapshot2.id
    assert snapshot1.serial_number == snapshot2.serial_number
    assert snapshot1.timestamp == snapshot2.timestamp
    assert snapshot1.custom_value == snapshot2.custom_value
    assert snapshot2.custom_value == 9
    assert '_id' in snapshot_dict1
    assert '_id' not in snapshot2.attr


def test_snapshot_diff():
    timestamp = datetime.now().timestamp()
    snapshot1 = Snapshot(id='1234', serial_number='5678', custom_value=9, timestamp=timestamp)
    snapshot2 = Snapshot(id='1234', serial_number='8765', custom_value=3, timestamp=timestamp + 1)
    diff = snapshot1.diff(snapshot2)
    assert set(diff.keys()) == {'serial_number', 'custom_value', 'timestamp'}
    assert diff['serial_number'] == '8765'
    assert diff['custom_value'] == 3
    assert diff['timestamp'] == timestamp + 1


def test_code_snapshot_to_dict_and_from_dict(default_os_env):
    code1 = Code(mul2)
    code2 = Code(add2)
    code3 = Code(add3, image='test-image')
    code1 >> code3
    code2 >> code3
    code3.receive('c') << code2
    code3(1, b=2)
    snapshot1 = CodeSnapshot(code3, args=(1, ), kwargs={'b': 2})
    snapshot_dict1 = snapshot1.to_dict()
    assert 'state' in snapshot_dict1
    assert snapshot_dict1['state'] == 'WAITING'
    assert 'source' in snapshot_dict1
    assert snapshot_dict1['source'] == code3.source
    assert 'dependency' in snapshot_dict1
    for dependency in snapshot_dict1['dependency']:
        code3_dependency = code3.dependency[dependency['serial_number']]
        assert set(dependency.keys()) == {'id', 'serial_number', 'param'}
        assert dependency['id'] == code3_dependency.id
        assert dependency['serial_number'] == code3_dependency.serial_number
        assert dependency['param'] == code3_dependency.param
    assert 'image' in snapshot_dict1
    assert snapshot_dict1['image'] is 'test-image'
    assert 'env' in snapshot_dict1
    assert snapshot_dict1['env'] is None
    assert 'owner' in snapshot_dict1
    assert snapshot_dict1['owner'] is None
    snapshot2 = CodeSnapshot.from_dict(snapshot_dict1)
    assert snapshot1.id == snapshot2.id
    assert snapshot1.serial_number == snapshot2.serial_number
    assert snapshot1.timestamp == snapshot2.timestamp
    assert snapshot1.args == snapshot2.args
    assert snapshot1.kwargs == snapshot2.kwargs
    assert snapshot1.source == snapshot2.source
    assert snapshot1.dependency == snapshot2.dependency
    assert snapshot1.image == snapshot2.image
    assert snapshot1.owner == snapshot2.owner


def test_code_snapshot_diff(default_os_env):
    timestamp = datetime.now().timestamp()
    code1 = Code(mul2)
    code2 = Code(add2)
    code3 = Code(add3)
    code1 >> code3
    code2 >> code3
    code3.receive('c') << code2
    code3(1, b=2)
    snapshot1 = CodeSnapshot(code3, args=(1, ), kwargs={'b': 2}, timestamp=timestamp)
    snapshot2 = CodeSnapshot.from_dict(snapshot1.to_dict())
    snapshot3 = CodeSnapshot(code3, args=(1, ), kwargs={'b': 3}, timestamp=timestamp + 1)
    assert snapshot1.diff(snapshot2) == dict()
    assert snapshot2.diff(snapshot1) == dict()
    diff = snapshot1.diff(snapshot3)
    assert set(diff.keys()) == {'kwargs', 'timestamp'}
    assert diff['kwargs'] == {'b': 3}
    assert diff['timestamp'] == timestamp + 1


def test_code_to_snapshot_and_from_snapshot(default_os_env):
    code1 = Code(add2)
    snapshot1 = code1.to_snapshot(args=(1, 2))
    code2 = Code.from_snapshot(snapshot=snapshot1)
    assert code2.get_state() == 'UNKNOWN'
    assert code2(1, 2) == 3


def test_codepack_to_snapshot_and_from_snapshot(default_os_env):
    c1 = Code(add3)
    c2 = Code(mul2)
    c3 = Code(combination)
    c4 = Code(linear)
    c5 = Code(print_x)

    c1 >> c3
    c2 >> c3
    c3 >> [c4, c5]

    c3.receive('c') << c1
    c4.receive('c') << c3
    c5.receive('x') << c3

    cp1 = CodePack(id='test_codepack', code=c1, subscribe=c4)

    argpack = cp1.make_argpack()
    argpack['add3']['a'] = 1
    argpack['add3']['b'] = 2
    argpack['add3']['c'] = 3
    argpack['mul2']['a'] = 2
    argpack['mul2']['b'] = 1
    argpack['combination']['a'] = 2
    argpack['combination']['b'] = 5
    argpack['linear']['b'] = 7
    argpack['linear']['a'] = 5

    ret = None
    with pytest.raises(TypeError):
        ret = cp1(argpack)

    assert ret is None
    assert cp1.get_state() == 'ERROR'
    snapshot1 = cp1.to_snapshot(argpack=argpack)
    assert snapshot1.owner is None
    snapshot1['owner'] = 'codepack'
    assert snapshot1.owner == 'codepack'
    cp2 = CodePack.from_snapshot(snapshot1)
    assert cp2.owner == 'codepack'
    assert cp1.id == cp2.id
    assert cp1.serial_number == cp2.serial_number
    assert cp1.get_state() == cp2.get_state()
    cp1_source = cp1.get_source()
    cp2_source = cp2.get_source()
    assert cp1_source.keys() == cp2_source.keys()
    for id in cp1_source.keys():
        assert cp1_source[id].strip() == cp2_source[id].strip()
    # assert cp1.get_structure() == cp2.get_structure()
    assert cp1.subscribe == cp2.subscribe
    assert set(cp1.codes.keys()) == set(cp2.codes.keys())
    assert cp1.owner is None
    assert cp2.owner == 'codepack'
    for code_id in cp1.codes.keys():
        code1 = cp1.codes[code_id]
        code2 = cp2.codes[code_id]
        assert code1.serial_number == code2.serial_number
        assert code1.parents.keys() == code2.parents.keys()
        for id in code1.parents.keys():
            assert code1.parents[id].serial_number == code2.parents[id].serial_number
        assert code1.children.keys() == code2.children.keys()
        for id in code1.children.keys():
            assert code1.children[id].serial_number == code2.children[id].serial_number
        assert code1.get_state() == code2.get_state()
        assert code1.dependency.keys() == code2.dependency.keys()
        for serial_number in code1.dependency.keys():
            assert code1.dependency[serial_number].id == code2.dependency[serial_number].id
            assert code1.dependency[serial_number].serial_number == code2.dependency[serial_number].serial_number
            assert code1.dependency[serial_number].param == code2.dependency[serial_number].param
