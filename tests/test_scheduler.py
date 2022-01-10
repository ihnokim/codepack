from codepack import Code, CodePack
from codepack.scheduler import MongoScheduler
from tests import *


def test_mongo_jobstore_codepack_snapshot(default_os_env, fake_mongodb):
    code = Code(hello)
    codepack = CodePack('codepack_test', code=code, subscribe=code)
    argpack = codepack.make_argpack()
    argpack['hello']['name'] = 'CodePack'
    job_id = 'job_test'
    db = 'test'
    collection = 'scheduler'
    scheduler = MongoScheduler(db=db, collection=collection, mongodb=fake_mongodb, blocking=False)
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
