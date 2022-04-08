from codepack.plugins.storable_job import StorableJob
from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError
from apscheduler.util import datetime_to_utc_timestamp, utc_timestamp_to_datetime
from datetime import datetime
from typing import Optional, TypeVar
try:
    import cPickle as pickle
except ImportError:  # pragma: nocover
    import pickle


Job = TypeVar('Job', bound='apscheduler.job.Job')
Storage = TypeVar('Storage', bound='codepack.storages.storage.Storage')


class JobStore(BaseJobStore):
    def __init__(self, storage: Storage, pickle_protocol: int = pickle.HIGHEST_PROTOCOL) -> None:
        super().__init__()
        self.storage = storage
        self.pickle_protocol = pickle_protocol

    def lookup_job(self, job_id: str) -> Optional[Job]:
        item = self.storage.load(key=job_id)
        if item:
            item.bind_job(scheduler=self._scheduler, alias=self._alias)
            return item.job
        else:
            return None

    def _get_sorted_items(self) -> list:
        all_item_keys = self.storage.list_all()
        all_items = self.storage.load(key=all_item_keys)
        return sorted(all_items, key=lambda x: x.job.next_run_time)

    def get_due_jobs(self, now: datetime) -> list:
        now_timestamp = datetime_to_utc_timestamp(now)
        pending = list()
        sorted_items = self._get_sorted_items()
        for item in sorted_items:
            _next_run_time = datetime_to_utc_timestamp(item.job.next_run_time)
            if _next_run_time is None or _next_run_time > now_timestamp:
                break
            item.bind_job(scheduler=self._scheduler, alias=self._alias)
            pending.append(item.job)
        return pending

    def get_next_run_time(self) -> Optional[datetime]:
        sorted_items = self._get_sorted_items()
        for item in sorted_items:
            _next_run_time = datetime_to_utc_timestamp(item.job.next_run_time)
            if _next_run_time is not None:
                return utc_timestamp_to_datetime(_next_run_time)
        return None

    def get_all_jobs(self) -> list:
        all_item_keys = self.storage.list_all()
        all_items = self.storage.load(key=all_item_keys)
        for item in all_items:
            item.bind_job(scheduler=self._scheduler, alias=self._alias)
        return [item.job for item in all_items]

    def add_job(self, job: Job) -> None:
        if self.storage.exist(key=job.id):
            raise ConflictingIdError(job.id)
        item = StorableJob(job=job)
        self.storage.save(item=item, update=True)

    def update_job(self, job: Job) -> None:
        item = StorableJob(job=job)
        self.storage.save(item=item, update=True)

    def remove_job(self, job_id: str) -> None:
        if not self.storage.exist(key=job_id):
            raise JobLookupError(job_id)
        self.storage.remove(key=job_id)

    def remove_all_jobs(self) -> None:
        all_item_keys = self.storage.list_all()
        self.storage.remove(key=all_item_keys)
