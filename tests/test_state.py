from codepack import Code, State, Snapshot
from tests import *


def test_state_eq():
    snapshot1 = Snapshot(id='test1', serial_number='1234', state='WAITING')
    snapshot2 = Snapshot(id='test2', serial_number='5678', state='RUNNING')
    state_code = State.WAITING
    assert snapshot1['state'] == 'WAITING'
    assert snapshot1['state'] == State.WAITING
    assert snapshot1['state'] == 3
    snapshot2['state'] = 'WAITING'
    assert snapshot1['state'] == snapshot2['state']
    assert state_code == 'WAITING'
    assert state_code == State.WAITING
    assert state_code == 3


def test_code_new(default_os_env):
    code1 = Code(add2)
    assert code1.get_state() == 'UNKNOWN'
    code2 = Code(add3, state='NEW')
    assert code2.get_state() == 'NEW'


def test_code_waiting(default_os_env):
    code1 = Code(add2)
    code2 = Code(add3)
    code1 >> code2
    code2.receive('b') << code1
    code2(3, c=2)
    assert code2.get_state() == 'WAITING'
    snapshot = code2.service['snapshot'].load(serial_number=code2.serial_number)
    assert 'args' in snapshot
    assert isinstance(snapshot['args'], list)
    assert len(snapshot['args']) == 1
    assert snapshot['args'][0] == 3
    assert 'kwargs' in snapshot
    assert isinstance(snapshot['kwargs'], dict)
    assert len(snapshot['kwargs']) == 1
    assert 'c' in snapshot['kwargs'].keys()
    assert snapshot['kwargs']['c'] == 2
