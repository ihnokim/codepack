from codepack.storages.storable import Storable
from apscheduler.job import Job
from apscheduler.util import datetime_to_utc_timestamp
from datetime import datetime, timezone
from bson.binary import Binary
try:
    import cPickle as pickle
except ImportError:  # pragma: nocover
    import pickle


class StorableJob(Storable):
    def __init__(self, job: Job) -> None:
        super().__init__(id=job.id, serial_number=job.id)
        self.job = job

    def to_dict(self) -> dict:
        utc_timestamp = datetime_to_utc_timestamp(self.job.next_run_time)
        return {'_id': self.id,
                'trigger': self.job.trigger.__str__(),
                'codepack': self.job.kwargs['snapshot']['id'],
                'argpack': self.job.kwargs['snapshot']['argpack']['_id'],
                'snapshot': self.job.kwargs['snapshot']['serial_number'],
                'last_run_time': datetime.now(timezone.utc).timestamp(),
                'next_run_time': utc_timestamp,
                'job_state': Binary(pickle.dumps(self.job.__getstate__(), pickle.HIGHEST_PROTOCOL))}

    @classmethod
    def from_dict(cls, d: dict) -> 'Storable':
        job_state = pickle.loads(d['job_state'])
        job = Job.__new__(Job)
        job.__setstate__(job_state)
        return cls(job=job)

    def bind_job(self, scheduler, alias):
        self.job._scheduler = scheduler
        self.job._jobstore_alias = alias
