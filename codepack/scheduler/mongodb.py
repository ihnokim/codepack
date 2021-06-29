from apscheduler.schedulers.background import BackgroundScheduler
from codepack.scheduler.jobstores.mongodb import MongoDBJobStore
from codepack.interface import MongoDB


class MongoScheduler:
    def __init__(self, db=None, collection=None, config=None, ssh_config=None, **kwargs):
        if config:
            self.config = config
            self.mongodb = MongoDB(config=config, ssh_config=ssh_config)
            self.db = db
            self.collection = collection
            jobstores = {
                'mongo': MongoDBJobStore(database=self.db,
                                         collection=self.collection,
                                         client=self.mongodb.client)
            }
            self.scheduler = BackgroundScheduler(jobstores=jobstores, **kwargs)
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

    def add_job(self, func, id, trigger, **kwargs):
        return self.scheduler.add_job(func=func, id=id, trigger=trigger, **kwargs)

    def remove_job(self, id, **kwargs):
        return self.scheduler.remove_job(job_id=id, **kwargs)
