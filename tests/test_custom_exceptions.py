from codepack import Code
from codepack.utils.exceptions import NewState, WaitingState
from codepack.plugins.state import State


def change_to_waiting_state():
    raise WaitingState('I am waiting!')


def change_to_new_state():
    raise NewState('I am new!')


def test_code_custom_exceptions_to_change_state(default_os_env):
    code1 = Code(change_to_waiting_state)
    assert code1.get_state() == State.UNKNOWN
    code1()
    assert code1.get_state() == State.WAITING
    assert code1.get_message() == 'I am waiting!'
    code2 = Code(change_to_new_state)
    assert code2.get_state() == State.UNKNOWN
    code2()
    assert code2.get_state() == State.NEW
    assert code2.get_message() == 'I am new!'
