from codepack.scheduler import Scheduler
from codepack.scheduler.jobstore import MongoJobStore
from codepack import CodePack
from codepack.storage import MongoStorage


class MongoScheduler(MongoStorage, Scheduler):
    def __init__(self, mongodb=None, db=None, collection=None, blocking=False, *args, **kwargs):
        MongoStorage.__init__(self, obj=CodePack, mongodb=mongodb, db=db, collection=collection, *args, **kwargs)
        Scheduler.__init__(self, blocking=blocking)

    def get_jobstore(self):
        return MongoJobStore(db=self.db, collection=self.collection, client=self.mongodb.session)
