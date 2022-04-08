from codepack import Code, SnapshotService, Snapshot, CodeSnapshot
from codepack.storages import MemoryStorage, FileStorage, MongoStorage
from datetime import datetime
from tests import *
import os


def test_memory_code_snapshot_service_save_and_load(default_os_env):
    storage = MemoryStorage(item_type=CodeSnapshot)
    mss = SnapshotService(storage=storage)
    mss.storage.init()
    code = Code(add2)
    snapshot = CodeSnapshot(code)
    mss.save(snapshot=snapshot)
    assert len(storage.memory) == 1
    assert mss.load(snapshot.serial_number) == snapshot.to_dict()
    loaded = mss.load(snapshot.serial_number, projection=['state'])
    assert set(loaded.keys()) == {'serial_number', 'state'}
    assert 'state' in loaded
    assert loaded['state'] == 'UNKNOWN'
    assert 'serial_number' in loaded
    assert loaded['serial_number'] == snapshot.serial_number


def test_memory_code_snapshot_service_search_and_remove(default_os_env):
    storage = MemoryStorage(item_type=CodeSnapshot)
    mss = SnapshotService(storage=storage)
    mss.storage.init()
    code1 = Code(add2)
    code2 = Code(add3)
    code1 >> code2
    code2(1, 2, 3)
    snapshot1 = CodeSnapshot(code1)
    snapshot2 = CodeSnapshot(code2)
    mss.save(snapshot=snapshot1)
    mss.save(snapshot=snapshot2)
    assert len(storage.memory) == 2
    loaded = mss.load([snapshot1.serial_number, snapshot2.serial_number, '1234'])
    assert len(loaded) == 2
    assert loaded[0]['state'] == 'UNKNOWN'
    assert loaded[1]['state'] == 'WAITING'
    loaded = mss.load([snapshot1.serial_number, snapshot2.serial_number, '1234'], projection=['state'])
    for x in loaded:
        assert set(x.keys()) == {'state', 'serial_number'}
    search_result = mss.search(key='state', value='WAITING')
    assert len(search_result) == 1
    assert search_result[0] == snapshot2.to_dict()
    search_result = mss.search(key='state', value='WAITING', projection=['state'])
    assert len(search_result) == 1
    assert len(search_result[0].keys()) == 2
    assert search_result[0]['state'] == 'WAITING'
    assert search_result[0]['serial_number'] == code2.serial_number
    mss.remove(snapshot2.serial_number)
    loaded = mss.load([snapshot1.serial_number, snapshot2.serial_number, '1234'])
    assert len(loaded) == 1
    search_result = mss.search(key='state', value='WAITING', projection=['state'])
    assert len(search_result) == 0


def test_memory_snapshot_service_update():
    storage = MemoryStorage(item_type=Snapshot)
    mss = SnapshotService(storage=storage)
    mss.storage.init()
    timestamp = datetime.now().timestamp()
    snapshot1 = Snapshot(id='1234', serial_number='5678', timestamp=timestamp)
    snapshot2 = Snapshot(id='1234', serial_number='5678', timestamp=timestamp + 1)
    mss.save(snapshot=snapshot1)
    mss.save(snapshot=snapshot2)
    assert len(storage.memory) == 1
    recent_instance = storage.memory[snapshot2.serial_number]
    assert id(snapshot1) != id(recent_instance)
    assert id(snapshot2) != id(recent_instance)
    assert snapshot1.id == recent_instance.id
    assert snapshot1.serial_number == recent_instance.serial_number
    assert snapshot1.timestamp + 1 == recent_instance.timestamp
    snapshot3 = Snapshot(id='1234', serial_number='5678', timestamp=timestamp + 1)
    mss.save(snapshot=snapshot3)
    recent_instance2 = storage.memory[snapshot2.serial_number]
    assert id(recent_instance) == id(recent_instance2)


def test_file_code_snapshot_service_save_and_load(default_os_env, testdir_snapshot_service):
    storage = FileStorage(item_type=CodeSnapshot, path=testdir_snapshot_service)
    fss = SnapshotService(storage=storage)
    code = Code(add2)
    snapshot = CodeSnapshot(code)
    fss.save(snapshot=snapshot)
    assert len(os.listdir(testdir_snapshot_service)) == 1
    assert fss.load(snapshot.serial_number) == snapshot.to_dict()
    loaded = fss.load(snapshot.serial_number, projection=['state'])
    assert set(loaded.keys()) == {'serial_number', 'state'}
    assert 'state' in loaded
    assert loaded['state'] == 'UNKNOWN'
    assert 'serial_number' in loaded
    assert loaded['serial_number'] == snapshot.serial_number


def test_file_code_snapshot_service_search_and_remove(default_os_env, testdir_snapshot_service):
    storage = FileStorage(item_type=CodeSnapshot, path=testdir_snapshot_service)
    fss = SnapshotService(storage=storage)
    code1 = Code(add2)
    code2 = Code(add3)
    code1 >> code2
    code2(1, 2, 3)
    snapshot1 = CodeSnapshot(code1)
    snapshot2 = CodeSnapshot(code2)
    fss.save(snapshot=snapshot1)
    fss.save(snapshot=snapshot2)
    assert len(os.listdir(testdir_snapshot_service)) == 2
    loaded = fss.load([snapshot1.serial_number, snapshot2.serial_number, '1234'])
    assert len(loaded) == 2
    assert loaded[0]['state'] == 'UNKNOWN'
    assert loaded[1]['state'] == 'WAITING'
    loaded = fss.load([snapshot1.serial_number, snapshot2.serial_number, '1234'], projection=['state'])
    for x in loaded:
        assert set(x.keys()) == {'state', 'serial_number'}
    search_result = fss.search(key='state', value='WAITING')
    assert len(search_result) == 1
    assert search_result[0] == snapshot2.to_dict()
    search_result = fss.search(key='state', value='WAITING', projection=['state'])
    assert len(search_result) == 1
    assert len(search_result[0].keys()) == 2
    assert search_result[0]['state'] == 'WAITING'
    assert search_result[0]['serial_number'] == code2.serial_number
    fss.remove(snapshot2.serial_number)
    loaded = fss.load([snapshot1.serial_number, snapshot2.serial_number, '1234'])
    assert len(loaded) == 1
    search_result = fss.search(key='state', value='WAITING', projection=['state'])
    assert len(search_result) == 0


def test_file_snapshot_service_update(testdir_snapshot_service):
    storage = FileStorage(item_type=Snapshot, path=testdir_snapshot_service)
    fss = SnapshotService(storage=storage)
    timestamp = datetime.now().timestamp()
    snapshot1 = Snapshot(id='1234', serial_number='5678', timestamp=timestamp)
    snapshot2 = Snapshot(id='1234', serial_number='5678', timestamp=timestamp + 1)
    fss.save(snapshot=snapshot1)
    fss.save(snapshot=snapshot2)
    assert len(os.listdir(testdir_snapshot_service)) == 1
    search_result = fss.search(key='id', value='1234', projection=['timestamp'])
    assert len(search_result) == 1
    assert search_result[0]['timestamp'] == timestamp + 1


def test_mongo_code_snapshot_service_save_and_load(default_os_env, fake_mongodb):
    db = 'test'
    collection = 'snapshot'
    storage = MongoStorage(item_type=CodeSnapshot, mongodb=fake_mongodb, db=db, collection=collection)
    mss = SnapshotService(storage=storage)
    code = Code(add2)
    snapshot = CodeSnapshot(code)
    mss.save(snapshot=snapshot)
    assert fake_mongodb[db][collection].count_documents({}) == 1
    assert mss.load(snapshot.serial_number) == snapshot.to_dict()
    loaded = mss.load(snapshot.serial_number, projection=['state'])
    assert set(loaded.keys()) == {'serial_number', 'state'}
    assert 'state' in loaded
    assert loaded['state'] == 'UNKNOWN'
    assert 'serial_number' in loaded
    assert loaded['serial_number'] == snapshot.serial_number


def test_mongo_code_snapshot_service_search_and_remove(default_os_env, fake_mongodb):
    db = 'test'
    collection = 'snapshot'
    storage = MongoStorage(item_type=CodeSnapshot, mongodb=fake_mongodb, db=db, collection=collection)
    mss = SnapshotService(storage=storage)
    code1 = Code(add2)
    code2 = Code(add3)
    code1 >> code2
    code2(1, 2, 3)
    snapshot1 = CodeSnapshot(code1)
    snapshot2 = CodeSnapshot(code2)
    mss.save(snapshot=snapshot1)
    mss.save(snapshot=snapshot2)
    assert fake_mongodb[db][collection].count_documents({}) == 2
    loaded = mss.load([snapshot1.serial_number, snapshot2.serial_number, '1234'])
    assert len(loaded) == 2
    assert loaded[0]['state'] == 'UNKNOWN'
    assert loaded[1]['state'] == 'WAITING'
    loaded = mss.load([snapshot1.serial_number, snapshot2.serial_number, '1234'], projection=['state'])
    for x in loaded:
        assert set(x.keys()) == {'state', 'serial_number'}
    search_result = mss.search(key='state', value='WAITING')
    assert len(search_result) == 1
    assert search_result[0] == snapshot2.to_dict()
    search_result = mss.search(key='state', value='WAITING', projection=['state'])
    assert len(search_result) == 1
    assert len(search_result[0].keys()) == 2
    assert search_result[0]['state'] == 'WAITING'
    assert search_result[0]['serial_number'] == code2.serial_number
    mss.remove(snapshot2.serial_number)
    loaded = mss.load([snapshot1.serial_number, snapshot2.serial_number, '1234'])
    assert len(loaded) == 1
    search_result = mss.search(key='state', value='WAITING', projection=['state'])
    assert len(search_result) == 0


def test_mongo_snapshot_service_update(fake_mongodb):
    db = 'test'
    collection = 'snapshot'
    storage = MongoStorage(item_type=Snapshot, mongodb=fake_mongodb, db=db, collection=collection)
    mss = SnapshotService(storage=storage)
    timestamp = datetime.now().timestamp()
    snapshot1 = Snapshot(id='1234', serial_number='5678', timestamp=timestamp)
    snapshot2 = Snapshot(id='1234', serial_number='5678', timestamp=timestamp + 1)
    mss.save(snapshot=snapshot1)
    mss.save(snapshot=snapshot2)
    assert fake_mongodb[db][collection].count_documents({}) == 1
    search_result = mss.search(key='id', value='1234', projection=['timestamp'])
    assert len(search_result) == 1
    assert search_result[0]['timestamp'] == timestamp + 1
