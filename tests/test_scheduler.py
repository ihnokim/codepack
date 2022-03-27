from codepack import Code, CodePack, Default, Scheduler
from codepack.storage.mongo_jobstore import MongoJobStore
from unittest.mock import patch
from tests import *
import os


@patch('pymongo.MongoClient')
def test_get_default_mongo_scheduler(mock_client):
    try:
        os.environ['CODEPACK_CONFIG_DIR'] = 'config'
        os.environ['CODEPACK_CONFIG_PATH'] = 'test.ini'
        scheduler = Default.get_scheduler()
        assert isinstance(scheduler, Scheduler)
        assert 'codepack' in scheduler.jobstores
        jobstore = scheduler.jobstores['codepack']
        assert isinstance(jobstore, MongoJobStore)
        assert scheduler.supervisor == 'http://localhost:8000'
        mock_client.assert_called_once_with(host='localhost', port=27017)
        mock_client().__getitem__.assert_called_once_with('codepack')
        mock_client().__getitem__().__getitem__.assert_called_once_with('scheduler')
    finally:
        os.environ.pop('CODEPACK_CONN_DIR', None)
        os.environ.pop('CODEPACK_CONN_PATH', None)


def test_mongo_jobstore_codepack_snapshot(default_os_env, fake_mongodb):
    code = Code(hello)
    codepack = CodePack('codepack_test', code=code, subscribe=code)
    argpack = codepack.make_argpack()
    argpack['hello']['name'] = 'CodePack'
    job_id = 'job_test'
    db = 'test'
    collection = 'scheduler'
    jobstore = MongoJobStore(mongodb=fake_mongodb, db=db, collection=collection)
    scheduler = Scheduler(jobstore=jobstore, blocking=False)
    scheduler.add_codepack(job_id=job_id, codepack=codepack, argpack=argpack, trigger='interval', seconds=30)
    scheduler.start()
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
