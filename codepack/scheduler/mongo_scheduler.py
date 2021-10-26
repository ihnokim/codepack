from apscheduler.schedulers.background import BackgroundScheduler
from codepack.scheduler.jobstores.mongodb import MongoDBJobStore
from codepack import CodePack
from codepack.abc import MongoDBService


class MongoScheduler(MongoDBService):
    def __init__(self, db=None, collection=None, config=None, ssh_config=None, mongodb=None, offline=False, **kwargs):
        super().__init__(db=db, collection=collection,
                         config=config, ssh_config=ssh_config, mongodb=mongodb, offline=offline)
        if self.mongodb:
            self.jobstores = {
                'mongo': MongoDBJobStore(database=self.db,
                                         collection=self.collection,
                                         client=self.mongodb.client)
            }
            self.scheduler = BackgroundScheduler(jobstores=self.jobstores, **kwargs)
        else:
            self.scheduler = BackgroundScheduler(**kwargs)
        self.scheduler.start()

    def __del__(self):
        self.shutdown()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()

    def shutdown(self):
        self.scheduler.shutdown()
        self.mongodb.close()

    def add_job(self, func, job_id, trigger, jobstore='mongo', **kwargs):
        return self.scheduler.add_job(func=func, id=job_id, trigger=trigger, jobstore=jobstore, **kwargs)

    def add_codepack(self, codepack, trigger, job_id=None, arg_dict=None, jobstore='mongo', **kwargs):
        if arg_dict is None:
            arg_dict = codepack.make_arg_dict()
        if job_id is None:
            job_id = codepack.id
        ret = self.add_job(self.run_codepack_dict, job_id=job_id, trigger=trigger, jobstore=jobstore,
                           kwargs={'codepack_dict': codepack.to_dict(), 'arg_dict': arg_dict}, **kwargs)
        if hasattr(self, 'mongodb'):
            self.mongodb[self.db][self.collection].update_one({'_id': job_id},
                                                              {'$set': {'codepack': codepack.id, 'arg_dict': arg_dict}})
        return ret

    def add_codepack_from_db(self, id, db, collection, trigger, config, ssh_config=None, job_id=None, arg_dict=None, jobstore='mongo', **kwargs):
        codepack = CodePack.from_db(id=id, db=db, collection=collection, config=config, ssh_config=ssh_config)
        return self.add_codepack(codepack=codepack, trigger=trigger, job_id=job_id, arg_dict=arg_dict, jobstore=jobstore, **kwargs)

    def remove_job(self, job_id, **kwargs):
        return self.scheduler.remove_job(job_id=job_id, **kwargs)

    @staticmethod
    def run_codepack_dict(codepack_dict, arg_dict=None):
        return CodePack.from_dict(codepack_dict)(arg_dict=arg_dict, lazy=False)
