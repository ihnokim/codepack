from apscheduler.schedulers.background import BackgroundScheduler
from codepack.scheduler.jobstore import MongoJobStore
from codepack import CodePack
from codepack.storage import MongoStorage


class MongoScheduler(MongoStorage):
    def __init__(self, mongodb=None, db=None, collection=None, *args, **kwargs):
        MongoStorage.__init__(self, obj=CodePack, mongodb=mongodb, db=db, collection=collection, *args, **kwargs)
        self.jobstores = {
            'mongo': MongoJobStore(db=self.db,
                                   collection=self.collection,
                                   client=self.mongodb.session)
        }
        self.scheduler = BackgroundScheduler(jobstores=self.jobstores, **kwargs)

    def start(self):
        self.scheduler.start()

    def stop(self):
        self.scheduler.shutdown()

    def add_job(self, func, job_id, trigger, **kwargs):
        return self.scheduler.add_job(func=func, id=job_id, trigger=trigger, jobstore='mongo', **kwargs)

    def add_codepack(self, codepack, trigger, job_id=None, argpack=None, **kwargs):
        if job_id is None:
            job_id = codepack.id
        ret = self.add_job(self.run_codepack_dict, job_id=job_id, trigger=trigger, jobstore='mongo',
                           kwargs={'codepack_dict': codepack.to_dict(), 'argpack': argpack}, **kwargs)
        if hasattr(self, 'mongodb'):
            self.mongodb[self.db][self.collection].update_one({'_id': job_id},
                                                              {'$set': {'codepack': codepack.id, 'argpack': argpack}})
        return ret

    '''
    def add_codepack_from_db(self, id, db, collection, trigger, config, ssh_config=None, job_id=None, arg_dict=None, jobstore='mongo', **kwargs):
        codepack = CodePack.from_db(id=id, db=db, collection=collection, config=config, ssh_config=ssh_config)
        return self.add_codepack(codepack=codepack, trigger=trigger, job_id=job_id, arg_dict=arg_dict, jobstore=jobstore, **kwargs)
    '''

    def remove_job(self, job_id, **kwargs):
        return self.scheduler.remove_job(job_id=job_id, **kwargs)

    @staticmethod
    def run_codepack_dict(codepack_dict, argpack=None):
        return CodePack.from_dict(codepack_dict)(argpack=argpack)
