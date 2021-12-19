from codepack.service import MemoryStateManager, FileStateManager, MongoStateManager
from codepack.utils.state import State, StateCode
import os


def test_state_eq():
    state1 = State(id='test1', serial_number='1234', state='WAITING')
    state2 = State(id='test2', serial_number='5678', state='RUNNING')
    state_code = StateCode.WAITING
    assert state1 == 'WAITING'
    assert state1 == StateCode.WAITING
    assert state1 == 3
    assert state1 == state1.to_dict()
    assert state1 == State.get_state_code(3)
    state2.set('WAITING')
    assert state1 == state2
    assert state_code == 'WAITING'
    assert state_code == StateCode.WAITING
    assert state_code == 3
    assert state_code == State.get_state_code(3)
    assert state_code == state1
    assert state_code == state1.to_dict()


def test_state_dict():
    state1 = State(id='test', serial_number='1234', state='WAITING')
    state_dict = {'id': 'test', '_id': '1234', 'state': 'WAITING'}
    state2 = State.from_dict(state_dict)
    assert state1.id == state2.id
    assert state1.serial_number == state2.serial_number
    assert state1.state == state2.state


def test_singleton_memory_state_manager():
    msm1 = MemoryStateManager()
    msm1.init()
    code_id = 'test'
    serial_number = '1234'
    state = 'RUNNING'
    msm1.set(id=code_id, serial_number=serial_number, state=state)
    msm2 = MemoryStateManager()
    assert msm1 == msm2
    assert len(msm2.states) == 1
    assert msm2.check(serial_number=serial_number)


def test_memory_state_manager_check():
    msm = MemoryStateManager()
    msm.init()
    code_id = 'test'
    serial_number = '1234'
    serial_numbers = ['123', '456', '789']
    state = 'RUNNING'
    msm.set(id=code_id, serial_number=serial_number, state=state)
    assert isinstance(msm.check(serial_number=serial_number), dict)
    msm.remove(serial_number=serial_number)
    msm.set(id=code_id, serial_number=serial_numbers[0], state=state)
    msm.set(id=code_id, serial_number=serial_numbers[1], state=state)
    check = msm.check(serial_number=serial_numbers)
    assert isinstance(check, list)
    assert len(check) == 2


def test_file_state_manager_check(testdir_state_manager):
    fsm = FileStateManager(path=testdir_state_manager)
    code_id = 'test'
    serial_number = '1234'
    serial_numbers = ['123', '456', '789']
    state = 'RUNNING'
    assert os.path.isdir(testdir_state_manager)
    fsm.set(id=code_id, serial_number=serial_number, state=state)
    assert isinstance(fsm.check(serial_number=serial_number), dict)
    fsm.remove(serial_number=serial_number)
    fsm.set(id=code_id, serial_number=serial_numbers[0], state=state)
    fsm.set(id=code_id, serial_number=serial_numbers[1], state=state)
    check = fsm.check(serial_number=serial_numbers)
    assert isinstance(check, list)
    assert len(check) == 2


def test_mongo_state_manager_check(fake_mongodb):
    db = 'test'
    collection = 'state'
    msm = MongoStateManager(mongodb=fake_mongodb, db=db, collection=collection)
    code_id = 'test'
    serial_number = '1234'
    serial_numbers = ['123', '456', '789']
    msm.remove(serial_number=serial_number)
    for s in serial_numbers:
        msm.remove(serial_number=s)
    state = 'RUNNING'
    msm.set(id=code_id, serial_number=serial_number, state=state)
    assert isinstance(msm.check(serial_number=serial_number), dict)
    msm.remove(serial_number=serial_number)
    msm.set(id=code_id, serial_number=serial_numbers[0], state=state)
    msm.set(id=code_id, serial_number=serial_numbers[1], state=state)
    check = msm.check(serial_number=serial_numbers)
    assert isinstance(check, list)
    assert len(check) == 2


def test_memory_state_manager():
    msm = MemoryStateManager()
    code_id = 'test'
    serial_number = '1234'
    state = 'RUNNING'
    msm.set(id=code_id, serial_number=serial_number, state=state)
    assert serial_number in msm.states, "'set' failed"
    check = msm.check(serial_number=serial_number)
    get = msm.get(serial_number=serial_number)
    assert check['id'] == code_id and check['_id'] == serial_number, "'check' failed"
    assert get == state, "'get' failed"
    msm.remove(serial_number=serial_number)
    check2 = msm.check(serial_number=serial_number)
    assert not check2, "'remove' failed"


def test_file_state_manager(testdir_state_manager):
    fsm = FileStateManager(path=testdir_state_manager)
    code_id = 'test'
    serial_number = '1234'
    state = 'RUNNING'
    fsm.set(id=code_id, serial_number=serial_number, state=state)
    assert os.path.isfile(State.get_path(serial_number=serial_number, path=fsm.path)), "'send' failed"
    check = fsm.check(serial_number=serial_number)
    get = fsm.get(serial_number=serial_number)
    assert check['id'] == code_id and check['_id'] == serial_number, "'check' failed"
    assert get == state, "'get' failed"
    fsm.remove(serial_number=serial_number)
    check2 = fsm.check(serial_number=serial_number)
    assert not check2, "'remove' failed"


def test_mongo_state_manager(fake_mongodb):
    db = 'test'
    collection = 'state'
    msm = MongoStateManager(mongodb=fake_mongodb, db=db, collection=collection)
    code_id = 'test'
    serial_number = '1234'
    state = 'RUNNING'
    msm.set(id=code_id, serial_number=serial_number, state=state)
    assert fake_mongodb[db][collection].find_one({'_id': serial_number}), "'send' failed"
    check = msm.check(serial_number=serial_number)
    get = msm.get(serial_number=serial_number)
    assert check['id'] == code_id and check['_id'] == serial_number, "'check' failed"
    assert get == state, "'get' failed"
    msm.remove(serial_number=serial_number)
    check2 = msm.check(serial_number=serial_number)
    assert not check2, "'remove' failed"
