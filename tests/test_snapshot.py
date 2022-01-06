from codepack.utils.snapshot import Snapshot
from codepack.utils.snapshot import CodeSnapshot
from codepack import Code
from tests import *
from datetime import datetime


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
    code3 = Code(add3)
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
        assert set(dependency.keys()) == {'id', 'serial_number', 'arg'}
        assert dependency['id'] == code3_dependency.id
        assert dependency['serial_number'] == code3_dependency.serial_number
        assert dependency['arg'] == code3_dependency.arg
    snapshot2 = CodeSnapshot.from_dict(snapshot_dict1)
    assert snapshot1.id == snapshot2.id
    assert snapshot1.serial_number == snapshot2.serial_number
    assert snapshot1.timestamp == snapshot2.timestamp
    assert snapshot1.args == snapshot2.args
    assert snapshot1.kwargs == snapshot2.kwargs
    assert snapshot1.source == snapshot2.source
    assert snapshot1.dependency == snapshot2.dependency


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
