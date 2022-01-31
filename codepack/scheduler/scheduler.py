import abc
from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler
from codepack import CodePack
from codepack.argpack import ArgPack
from codepack.snapshot import CodePackSnapshot
from codepack.employee import Supervisor
import requests
import json
import os


class Scheduler(metaclass=abc.ABCMeta):
    def __init__(self, callback=None, blocking=False):
        self.scheduler = None
        self.jobstores = dict()
        self.jobstores['codepack'] = self.get_jobstore()
        self.callback = None
        self.init_scheduler(blocking=blocking)
        self.register(callback)

    def register(self, callback):
        self.callback = callback

    def start(self):
        try:
            self.scheduler.start()
        except KeyboardInterrupt:
            self.stop()

    def init_scheduler(self, blocking=False, **kwargs):
        if blocking:
            self.scheduler = BlockingScheduler(jobstores=self.jobstores, **kwargs)
        else:
            self.scheduler = BackgroundScheduler(jobstores=self.jobstores, **kwargs)

    def is_running(self):
        return self.scheduler.running

    @abc.abstractmethod
    def get_jobstore(self):
        """get jobstore"""

    def stop(self):
        self.scheduler.shutdown()

    def add_job(self, func, job_id, trigger, **kwargs):
        return self.scheduler.add_job(func=func, id=job_id, trigger=trigger, jobstore='codepack', **kwargs)

    def add_codepack(self, codepack, trigger, job_id=None, argpack=None, callback=None, **kwargs):
        if job_id is None:
            job_id = codepack.id
        snapshot = codepack.to_snapshot(argpack=argpack)
        ret = self.add_job(self._get_callback(callback), job_id=job_id, trigger=trigger, kwargs={'snapshot': snapshot.to_dict()}, **kwargs)
        return ret

    def add_snapshot(self, snapshot, trigger, job_id=None, callback=None, **kwargs):
        if job_id is None:
            job_id = snapshot.id
        ret = self.add_job(self._get_callback(callback), job_id=job_id, trigger=trigger, kwargs={'snapshot': snapshot.to_dict()}, **kwargs)
        return ret

    def remove_job(self, job_id, **kwargs):
        return self.scheduler.remove_job(job_id=job_id, **kwargs)

    def _get_callback(self, callback):
        if callback:
            _callback = callback
        elif self.callback:
            _callback = self.callback
        else:
            _callback = self.run_snapshot
        return _callback

    @staticmethod
    def _get_snapshot(snapshot):
        if isinstance(snapshot, dict):
            _snapshot = CodePackSnapshot.from_dict(snapshot)
        elif isinstance(snapshot, CodePackSnapshot):
            _snapshot = snapshot
        elif isinstance(snapshot, str):
            _snapshot = CodePackSnapshot.from_json(snapshot)
        else:
            raise TypeError(type(snapshot))
        return _snapshot

    @staticmethod
    def _get_codepack(snapshot):
        return CodePack.from_snapshot(snapshot)

    @staticmethod
    def _get_argpack(snapshot):
        return ArgPack.from_dict(snapshot['argpack'])

    @staticmethod
    def request_to_supervisor(snapshot):
        _snapshot = Scheduler._get_snapshot(snapshot)
        return requests.post('%s/codepack/run/snapshot' % os.environ['CODEPACK_SCHEDULER_SUPERVISOR'],
                             data=json.dumps({'snapshot': _snapshot.to_json()}))

    @staticmethod
    def run_snapshot(snapshot):
        _snapshot = Scheduler._get_snapshot(snapshot)
        codepack = Scheduler._get_codepack(_snapshot)
        argpack = Scheduler._get_argpack(_snapshot)
        return codepack(argpack=argpack)
