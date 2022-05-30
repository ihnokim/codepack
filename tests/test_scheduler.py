from unittest.mock import patch
from codepack import Code, CodePack, ArgPack, Scheduler, JobStore, StorableJob, CodePackSnapshot
from codepack.storages import MemoryStorage, FileStorage, MongoStorage, S3Storage
from tests import add2, add3, hello


def test_memory_storage_jobstore_codepack_snapshot():
    code = Code(hello)
    codepack = CodePack('codepack_test', code=code, subscribe=code)
    argpack = codepack.make_argpack()
    argpack['hello']['name'] = 'CodePack'
    job_id = 'job_test'
    storage = MemoryStorage(item_type=StorableJob, key='id')
    jobstore = JobStore(storage=storage)
    scheduler = Scheduler(jobstore=jobstore, blocking=False)
    scheduler.add_codepack(job_id=job_id, codepack=codepack, argpack=argpack, trigger='interval', seconds=30)
    scheduler.start()
    assert scheduler.is_running()
    assert codepack.id not in storage.memory
    assert job_id in storage.memory
    job = storage.memory[job_id].to_dict()
    for k in ['_id', 'trigger', 'codepack', 'snapshot', 'last_run_time', 'next_run_time']:
        assert k in job
    assert job['_id'] == job_id
    assert job['trigger'] == 'interval[0:00:30]'
    assert job['codepack'] == codepack.id
    assert job['snapshot'] == codepack.serial_number
    scheduler.remove_job(job_id)
    assert len(storage.memory) == 0
    scheduler.stop()


def test_file_storage_jobstore_codepack_snapshot(testdir):
    code = Code(hello)
    codepack = CodePack('codepack_test', code=code, subscribe=code)
    argpack = codepack.make_argpack()
    argpack['hello']['name'] = 'CodePack'
    job_id = 'job_test'
    storage = FileStorage(item_type=StorableJob, key='id', path='testdir/scheduler/')
    jobstore = JobStore(storage=storage)
    scheduler = Scheduler(jobstore=jobstore, blocking=False)
    scheduler.add_codepack(job_id=job_id, codepack=codepack, argpack=argpack, trigger='interval', seconds=30)
    scheduler.start()
    assert scheduler.is_running()
    assert not storage.exist(key=codepack.id)
    assert storage.exist(key=job_id)
    job = storage.load(key=job_id, to_dict=True)
    for k in ['_id', 'trigger', 'codepack', 'snapshot', 'last_run_time', 'next_run_time']:
        assert k in job
    assert job['_id'] == job_id
    assert job['trigger'] == 'interval[0:00:30]'
    assert job['codepack'] == codepack.id
    assert job['snapshot'] == codepack.serial_number
    scheduler.remove_job(job_id)
    assert len(storage.list_all()) == 0
    scheduler.stop()


def test_mongo_storage_jobstore_codepack_snapshot(fake_mongodb):
    code = Code(hello)
    codepack = CodePack('codepack_test', code=code, subscribe=code)
    argpack = codepack.make_argpack()
    argpack['hello']['name'] = 'CodePack'
    job_id = 'job_test'
    db = 'test'
    collection = 'scheduler'
    storage = MongoStorage(item_type=StorableJob, key='id', mongodb=fake_mongodb, db=db, collection=collection)
    jobstore = JobStore(storage=storage)
    scheduler = Scheduler(jobstore=jobstore, blocking=False)
    scheduler.add_codepack(job_id=job_id, codepack=codepack, argpack=argpack, trigger='interval', seconds=30)
    scheduler.start()
    assert scheduler.is_running()
    assert fake_mongodb[db][collection].count_documents({'_id': codepack.id}) == 0
    assert fake_mongodb[db][collection].count_documents({'_id': job_id}) == 1
    document = fake_mongodb[db][collection].find_one({'_id': job_id})
    for k in ['_id', 'trigger', 'codepack', 'snapshot', 'last_run_time', 'next_run_time']:
        assert k in document
    assert document['_id'] == job_id
    assert document['trigger'] == 'interval[0:00:30]'
    assert document['codepack'] == codepack.id
    assert document['snapshot'] == codepack.serial_number
    scheduler.remove_job(job_id)
    assert fake_mongodb[db][collection].count_documents({'_id': job_id}) == 0
    scheduler.stop()


@patch('boto3.client')
def test_s3_storage_jobstore_codepack_snapshot(mock_client):
    storage = S3Storage(item_type=StorableJob, key='id', bucket='test_bucket', path='testdir/scheduler/', s3={})
    jobstore = JobStore(storage=storage)
    scheduler = Scheduler(jobstore=jobstore, blocking=False)
    assert hasattr(scheduler.jobstores['codepack'], 'storage')
    assert isinstance(scheduler.jobstores['codepack'].storage, S3Storage)
    assert scheduler.jobstores['codepack'].storage == storage


def test_scheduler_get_instances(default_os_env):
    code1 = Code(add2)
    code2 = Code(add3)
    code1 >> code2
    code2.receive('b') << code1
    codepack = CodePack('test-codepack', code=code1, subscribe=code2)
    argpack = codepack.make_argpack()
    argpack['add2'](a=3, b=5)
    argpack['add3'](a=2, c=7)
    snapshot = codepack.to_snapshot(argpack=argpack)
    _codepack = Scheduler._get_codepack(snapshot=snapshot)
    _argpack = Scheduler._get_argpack(snapshot=snapshot)
    _snapshot_from_snapshot = Scheduler._get_snapshot(snapshot=snapshot)
    _snapshot_from_dict = Scheduler._get_snapshot(snapshot=snapshot.to_dict())
    _snapshot_from_json = Scheduler._get_snapshot(snapshot=snapshot.to_json())
    assert isinstance(_codepack, CodePack)
    assert isinstance(_argpack, ArgPack)
    assert isinstance(_snapshot_from_snapshot, CodePackSnapshot)
    assert isinstance(_snapshot_from_dict, CodePackSnapshot)
    assert isinstance(_snapshot_from_json, CodePackSnapshot)
    assert codepack.id == _codepack.id
    assert argpack.id == _argpack.id
    assert snapshot.diff(_snapshot_from_snapshot) == {}
    assert snapshot.diff(_snapshot_from_dict) == {}
    assert snapshot.diff(_snapshot_from_json) == {}
    assert codepack.get_structure() == _codepack.get_structure()
    source1 = codepack.get_source()
    source2 = _codepack.get_source()
    for code_id in ['add2', 'add3']:
        assert source1[code_id].strip() == source2[code_id].strip()
        assert code_id in _argpack
    assert _argpack['add2']['a'] == 3 and _argpack['add2']['b'] == 5
    assert _argpack['add3']['a'] == 2 and _argpack['add3']['c'] == 7


def test_scheduler_run_snapshot(default_os_env):
    code1 = Code(add2)
    code2 = Code(add3)
    code1 >> code2
    code2.receive('b') << code1
    codepack = CodePack('test-codepack', code=code1, subscribe=code2)
    argpack = codepack.make_argpack()
    argpack['add2'](a=3, b=5)
    argpack['add3'](a=2, c=7)
    snapshot = codepack.to_snapshot(argpack=argpack)
    assert Scheduler.run_snapshot(snapshot=snapshot) == 17
