from codepack.service import MemorySnapshotService
from codepack.utils.snapshot import Snapshot
from codepack.utils.snapshot import CodeSnapshot
from codepack import Code
from datetime import datetime
from tests import *


def test_singleton_memory_snapshot_service():
    mss1 = MemorySnapshotService()
    mss1.init()
    snapshot = CodeSnapshot()
    mss1.save(snapshot=snapshot)
    mss2 = MemorySnapshotService()
    assert mss1 == mss2
    assert len(mss2.memory) == 1
    assert mss2.load(serial_number=snapshot.serial_number) == snapshot.to_dict()


def test_memory_snapshot_service_save_and_load(default_os_env):
    mss = MemorySnapshotService()
    mss.init()
    code = Code(add2)
    snapshot = CodeSnapshot(code)
    mss.save(snapshot=snapshot)
    assert len(mss.memory) == 1
    assert mss.load(snapshot.serial_number) == snapshot.to_dict()
    loaded = mss.load(snapshot.serial_number, projection={'state'})
    assert set(loaded.keys()) == {'serial_number', 'state'}
    assert 'state' in loaded
    assert loaded['state'] == 'UNKNOWN'
    assert 'serial_number' in loaded
    assert loaded['serial_number'] == snapshot.serial_number


def test_memory_snapshot_service_search_and_remove(default_os_env):
    mss = MemorySnapshotService()
    mss.init()
    code1 = Code(add2)
    code2 = Code(add3)
    code1 >> code2
    code2(1, 2, 3)
    snapshot1 = CodeSnapshot(code1)
    snapshot2 = CodeSnapshot(code2)
    mss.save(snapshot=snapshot1)
    mss.save(snapshot=snapshot2)
    assert len(mss.memory) == 2
    loaded = mss.load([snapshot1.serial_number, snapshot2.serial_number, '1234'])
    assert len(loaded) == 2
    assert loaded[0]['state'] == 'UNKNOWN'
    assert loaded[1]['state'] == 'WAITING'
    loaded = mss.load([snapshot1.serial_number, snapshot2.serial_number, '1234'], projection={'state'})
    for x in loaded:
        assert set(x.keys()) == {'state', 'serial_number'}
    search_result = mss.search(key='state', value='WAITING')
    assert len(search_result) == 1
    assert search_result[0] == snapshot2.to_dict()
    search_result = mss.search(key='state', value='WAITING', projection={'state'})
    assert len(search_result) == 1
    assert len(search_result[0].keys()) == 2
    assert search_result[0]['state'] == 'WAITING'
    assert search_result[0]['serial_number'] == code2.serial_number
    mss.remove(snapshot2.serial_number)
    loaded = mss.load([snapshot1.serial_number, snapshot2.serial_number, '1234'])
    assert len(loaded) == 1
    search_result = mss.search(key='state', value='WAITING', projection={'state'})
    assert len(search_result) == 0


def test_memory_snapshot_service_update():
    mss = MemorySnapshotService()
    mss.init()
    timestamp = datetime.now().timestamp()
    snapshot1 = Snapshot(id='1234', serial_number='5678', timestamp=timestamp)
    snapshot2 = Snapshot(id='1234', serial_number='5678', timestamp=timestamp + 1)
    mss.save(snapshot=snapshot1)
    tmp_id = id(mss.memory[snapshot1.serial_number])
    mss.save(snapshot=snapshot2)
    assert len(mss.memory) == 1
    assert tmp_id == id(mss.memory[snapshot2.serial_number])
    assert mss.memory[snapshot1.serial_number].timestamp == timestamp + 1
