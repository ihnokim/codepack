from apscheduler.jobstores.mongodb import *
from datetime import datetime, timezone


class MongoJobStore(MongoDBJobStore):
    def __init__(self, db,
                 collection,
                 client=None,
                 pickle_protocol=pickle.HIGHEST_PROTOCOL,
                 **connect_args):
        super().__init__(database=db,
                         collection=collection,
                         client=client,
                         pickle_protocol=pickle_protocol,
                         **connect_args)

    def add_job(self, job):
        utc_timestamp = datetime_to_utc_timestamp(job.next_run_time)
        try:
            self.collection.insert_one({
                '_id': job.id,
                'trigger': job.trigger.__str__(),
                'codepack': job.kwargs['snapshot']['id'],
                'argpack': job.kwargs['snapshot']['argpack']['_id'],
                'snapshot': job.kwargs['snapshot']['serial_number'],
                'last_run_time': datetime.now(timezone.utc).timestamp(),
                'next_run_time': utc_timestamp,
                'job_state': Binary(pickle.dumps(job.__getstate__(), self.pickle_protocol))
            })
        except DuplicateKeyError:
            raise ConflictingIdError(job.id)

    def update_job(self, job):
        utc_timestamp = datetime_to_utc_timestamp(job.next_run_time)
        changes = {
            'last_run_time': datetime.now(timezone.utc).timestamp(),
            'next_run_time': utc_timestamp,
            'job_state': Binary(pickle.dumps(job.__getstate__(), self.pickle_protocol))
        }
        result = self.collection.update_one({'_id': job.id}, {'$set': changes})
        if result and result.matched_count == 0:
            raise JobLookupError(job.id)
