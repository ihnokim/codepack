from codepack import Default, Code, CodePack
from codepack.storages import MemoryMessenger
from tests import *


def test_memory_supervisor_run_code():
    supervisor = Default.get_employee('supervisor')
    assert isinstance(supervisor.messenger, MemoryMessenger)
    assert supervisor.messenger.topic == 'codepack'
    assert supervisor.messenger.queues['codepack'].empty()
    code = Code(add2)
    sn = supervisor.run_code(code=code, args=(3,), kwargs={'b': 5})
    assert sn == code.serial_number
    assert not supervisor.messenger.queues['codepack'].empty() and supervisor.messenger.queues['codepack'].qsize() == 1
    item = supervisor.messenger.queues['codepack'].get(block=False)
    assert item == code.to_snapshot(args=(3,), kwargs={'b': 5}, timestamp=item['timestamp']).to_dict()
    supervisor.close()


def test_memory_supervisor_run_codepack():
    supervisor = Default.get_employee('supervisor')
    assert isinstance(supervisor.messenger, MemoryMessenger)
    assert supervisor.messenger.topic == 'codepack'
    assert supervisor.messenger.queues['codepack'].empty()
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(combination)
    code1 >> code3
    code2 >> code3
    code3.receive('c') << code1
    code3.receive('d') << code2
    codepack = CodePack('test', code=code1, subscribe=code3)
    argpack = codepack.make_argpack()
    argpack['add2'](a=3, b=5)
    argpack['mul2'](a=2, b=3)
    argpack['combination'](a=2, b=4)
    sn = supervisor.run_codepack(codepack=codepack, argpack=argpack)
    assert sn == codepack.serial_number
    assert not supervisor.messenger.queues['codepack'].empty() and supervisor.messenger.queues['codepack'].qsize() == 3
    items = [supervisor.messenger.queues['codepack'].get(block=False) for _ in range(3)]
    snapshots = [c.to_snapshot(kwargs=argpack[i].to_dict()).to_dict() for i, c in codepack.codes.items()]
    for item in items:
        item.pop('timestamp', None)
    for snapshot in snapshots:
        snapshot.pop('timestamp', None)
    assert sorted(items, key=lambda x: x['id']) == sorted(snapshots, key=lambda x: x['id'])
    supervisor.close()


def test_memory_supervisor_organize():
    supervisor = Default.get_employee('supervisor')
    assert isinstance(supervisor.messenger, MemoryMessenger)
    assert supervisor.messenger.topic == 'codepack'
    assert supervisor.messenger.queues['codepack'].empty()
    code1 = Code(add2)
    code2 = Code(mul2)
    code1 >> code2
    code2(3, b=5)
    assert code1.get_state() == 'UNKNOWN'
    assert code2.get_state() == 'WAITING'
    supervisor.organize()
    assert code1.get_state() == 'UNKNOWN'
    assert code2.get_state() == 'WAITING'
    assert supervisor.messenger.queues['codepack'].empty()
    code1(a=2, b=4)
    assert code1.get_state() == 'TERMINATED'
    assert code2.get_state() == 'WAITING'
    assert supervisor.messenger.queues['codepack'].empty()
    supervisor.organize()
    assert code1.get_state() == 'TERMINATED'
    assert code2.get_state() == 'READY'
    assert not supervisor.messenger.queues['codepack'].empty() and supervisor.messenger.queues['codepack'].qsize() == 1
    item = supervisor.messenger.queues['codepack'].get(block=False)
    item.pop('source', None)
    expected_item = code2.to_snapshot(args=(3,), kwargs={'b': 5}, timestamp=item['timestamp']).to_dict()
    expected_item.pop('source', None)
    assert item == expected_item
    supervisor.close()
