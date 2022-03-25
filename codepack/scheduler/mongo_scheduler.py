from codepack.scheduler.scheduler import Scheduler
from codepack.scheduler.jobstore.mongo_jobstore import MongoJobStore
from codepack.storage.mongo_storage import MongoStorage


class MongoScheduler(MongoStorage, Scheduler):
    def __init__(self, mongodb=None, db=None, collection=None, blocking=False, callback=None, supervisor=None, *args, **kwargs):
        MongoStorage.__init__(self, mongodb=mongodb, db=db, collection=collection, *args, **kwargs)
        Scheduler.__init__(self, blocking=blocking, callback=callback, supervisor=supervisor)

    def get_jobstore(self):
        return MongoJobStore(db=self.db, collection=self.collection, client=self.mongodb.session)
