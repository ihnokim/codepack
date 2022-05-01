from codepack.codepack import CodePack
from codepack.argpack import ArgPack
from codepack.plugins.snapshots.codepack_snapshot import CodePackSnapshot
from codepack.plugins.supervisor import Supervisor
from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler
import requests
import json
from typing import Optional, Union, Callable, TypeVar, Any


BaseJobStore = TypeVar('BaseJobStore', bound='apscheduler.jobstores.base.BaseJobStore')
BaseTrigger = TypeVar('BaseTrigger', bound='apscheduler.triggers.base.BaseTrigger')
Job = TypeVar('Job', bound='apscheduler.job.Job')
Response = TypeVar('Response', bound='requests.models.Response')


class Scheduler:
    supervisor = None

    def __init__(self, callback: Optional[Callable] = None, jobstore: Optional[BaseJobStore] = None,
                 blocking: bool = False, supervisor: Optional[Union[Supervisor, str]] = None) -> None:
        self.scheduler = None
        self.jobstores = dict()
        if jobstore:
            self.jobstores['codepack'] = jobstore
        self.blocking = blocking
        self.callback = None
        self.register(callback)
        self.init_scheduler(blocking=self.blocking)
        self._shutdown = False
        if isinstance(supervisor, str) or isinstance(supervisor, Supervisor) or supervisor is None:
            self.init_supervisor(supervisor=supervisor)
        else:
            raise TypeError(type(supervisor))  # pragma: no cover

    def init_scheduler(self, **kwargs: Any) -> None:
        if self.blocking:
            self.scheduler = BlockingScheduler(jobstores=self.jobstores, **kwargs)
        else:
            self.scheduler = BackgroundScheduler(jobstores=self.jobstores, **kwargs)

    @classmethod
    def init_supervisor(cls, supervisor: Optional[Union[Supervisor, str]] = None):
        Scheduler.supervisor = supervisor

    def register(self, callback: Optional[Callable] = None):
        self.callback = callback

    def start(self) -> None:
        try:
            self.scheduler.start()
        except KeyboardInterrupt:
            self.stop()

    def is_running(self) -> bool:
        return self.scheduler.running

    def stop(self) -> None:
        if self.scheduler:
            self.scheduler.shutdown()

    def add_job(self, func: Callable, job_id: str, trigger: Union[BaseTrigger, str], **kwargs: Any) -> Job:
        return self.scheduler.add_job(func=func, id=job_id, trigger=trigger, jobstore='codepack', **kwargs)

    def add_codepack(self, codepack: CodePack, trigger: Union[BaseTrigger, str],
                     job_id: Optional[str] = None, argpack: Optional[Union[ArgPack, dict]] = None,
                     callback: Optional[Callable] = None, **kwargs: Any) -> Job:
        if job_id is None:
            job_id = codepack.id
        snapshot = codepack.to_snapshot(argpack=argpack)
        return self.add_job(self._get_callback(callback), job_id=job_id, trigger=trigger,
                            kwargs={'snapshot': snapshot.to_dict()}, **kwargs)

    def add_snapshot(self, snapshot: CodePackSnapshot, trigger: Union[BaseTrigger, str], job_id: Optional[str] = None,
                     callback: Optional[Callable] = None, **kwargs: Any) -> Job:
        if job_id is None:
            job_id = snapshot.id
        return self.add_job(self._get_callback(callback), job_id=job_id, trigger=trigger,
                            kwargs={'snapshot': snapshot.to_dict()}, **kwargs)

    def remove_job(self, job_id: str, **kwargs: Any) -> None:
        self.scheduler.remove_job(job_id=job_id, **kwargs)

    def _get_callback(self, callback: Optional[Callable] = None) -> Callable:
        if callback:
            _callback = callback
        elif self.callback:
            _callback = self.callback
        elif self.supervisor:
            _callback = self.request_to_supervisor
        else:
            _callback = self.run_snapshot
        return _callback

    @staticmethod
    def _get_snapshot(snapshot: Union[CodePackSnapshot, dict, str]) -> CodePackSnapshot:
        if isinstance(snapshot, CodePackSnapshot):
            _snapshot = snapshot
        elif isinstance(snapshot, dict):
            _snapshot = CodePackSnapshot.from_dict(snapshot)
        elif isinstance(snapshot, str):
            _snapshot = CodePackSnapshot.from_json(snapshot)
        else:
            raise TypeError(type(snapshot))  # pragma: no cover
        return _snapshot

    @staticmethod
    def _get_codepack(snapshot: CodePackSnapshot) -> CodePack:
        return CodePack.from_snapshot(snapshot)

    @staticmethod
    def _get_argpack(snapshot: Union[CodePackSnapshot, dict]) -> ArgPack:
        return ArgPack.from_dict(snapshot['argpack'])

    @staticmethod
    def request_to_supervisor(snapshot: Union[CodePackSnapshot, dict]) -> Union[Response, str]:
        _snapshot = Scheduler._get_snapshot(snapshot)
        if isinstance(Scheduler.supervisor, str):
            return requests.post('%s/codepack/run/snapshot' % Scheduler.supervisor,
                                 data=json.dumps({'snapshot': _snapshot.to_json()}))
        else:
            codepack = Scheduler._get_codepack(_snapshot)
            argpack = Scheduler._get_argpack(_snapshot)
            return Scheduler.supervisor.run_codepack(codepack=codepack, argpack=argpack)

    @staticmethod
    def run_snapshot(snapshot: Union[CodePackSnapshot, dict, str]) -> Any:
        _snapshot = Scheduler._get_snapshot(snapshot)
        codepack = Scheduler._get_codepack(_snapshot)
        argpack = Scheduler._get_argpack(_snapshot)
        return codepack(argpack=argpack)
