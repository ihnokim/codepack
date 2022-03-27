from codepack.interface.mongodb import MongoDB
from apscheduler.jobstores.mongodb import *
from datetime import datetime, timezone
from typing import Union


class MongoJobStore(MongoDBJobStore):
    def __init__(self, mongodb: Union[MongoDB, dict], db: str, collection: str,
                 pickle_protocol=pickle.HIGHEST_PROTOCOL, *args, **kwargs):
        if isinstance(mongodb, MongoDB):
            _client = mongodb.session
        elif isinstance(mongodb, dict):
            _mongodb = MongoDB(mongodb, *args, **kwargs)
            _client = _mongodb.session
        else:
            raise TypeError(type(mongodb))  # pragma: no cover
        super().__init__(database=db,
                         collection=collection,
                         client=_client,
                         pickle_protocol=pickle_protocol)

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
