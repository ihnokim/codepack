import abc
from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler
from codepack import CodePack
from codepack.snapshot import CodePackSnapshot


class Scheduler(metaclass=abc.ABCMeta):
    def __init__(self, blocking=False):
        self.scheduler = None
        self.jobstores = dict()
        self.jobstores['codepack'] = self.get_jobstore()
        self.init_scheduler(blocking=blocking)

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

    @abc.abstractmethod
    def get_jobstore(self):
        """get jobstore"""

    def stop(self):
        self.scheduler.shutdown()

    def add_job(self, func, job_id, trigger, **kwargs):
        return self.scheduler.add_job(func=func, id=job_id, trigger=trigger, jobstore='codepack', **kwargs)

    def add_codepack(self, codepack, trigger, job_id=None, argpack=None, **kwargs):
        if job_id is None:
            job_id = codepack.id
        snapshot = codepack.to_snapshot(argpack=argpack)
        ret = self.add_job(self.run_codepack, job_id=job_id, trigger=trigger, kwargs={'snapshot': snapshot.to_dict()}, **kwargs)
        return ret

    def remove_job(self, job_id, **kwargs):
        return self.scheduler.remove_job(job_id=job_id, **kwargs)

    @staticmethod
    def run_codepack(snapshot):
        if isinstance(snapshot, dict):
            codepack = CodePack.from_snapshot(CodePackSnapshot.from_dict(snapshot))
        elif isinstance(snapshot, CodePackSnapshot):
            codepack = CodePack.from_snapshot(snapshot)
        else:
            raise TypeError(type(snapshot))
        return codepack(argpack=snapshot['argpack'])
