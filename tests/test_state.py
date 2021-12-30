from codepack.utils.state import State, StateCode
from codepack import Code
from tests import *


def test_state_eq():
    state1 = State(id='test1', serial_number='1234', state='WAITING')
    state2 = State(id='test2', serial_number='5678', state='RUNNING')
    state_code = StateCode.WAITING
    assert state1 == 'WAITING'
    assert state1 == StateCode.WAITING
    assert state1 == 3
    assert state1 == State.get_state_code(3)
    state2.set('WAITING')
    assert state1 == state2
    assert state_code == 'WAITING'
    assert state_code == StateCode.WAITING
    assert state_code == 3
    assert state_code == State.get_state_code(3)


def test_state_dict():
    state1 = State(id='test', serial_number='1234', state='WAITING')
    state_dict = {'id': 'test', '_id': '1234', 'state': 'WAITING'}
    state2 = State.from_dict(state_dict)
    assert state1.id == state2.id
    assert state1.serial_number == state2.serial_number
    assert state1.state == state2.state


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
    state_dict = code2.service['state_manager'].check(code2.serial_number)
    assert 'args' in state_dict
    assert isinstance(state_dict['args'], list)
    assert len(state_dict['args']) == 1
    assert state_dict['args'][0] == 3
    assert 'kwargs' in state_dict
    assert isinstance(state_dict['kwargs'], dict)
    assert len(state_dict['kwargs']) == 1
    assert 'c' in state_dict['kwargs'].keys()
    assert state_dict['kwargs']['c'] == 2
