from typing import Any, Optional
from codepack.jobs.code_snapshot import CodeSnapshot
from codepack.jobs.job import Job
from codepack.code import Code
from codepack.jobs.codepack_snapshot import CodePackSnapshot
from codepack.jobs.states import States
from codepack.jobs.states.state import State
from codepack.arg import Arg
from codepack.codepack import CodePack
from codepack.argpack import ArgPack
from codepack.jobs.job_manager import JobManager
from codepack.jobs.result_cache import ResultCache
from codepack.utils.observable import Observable
from codepack.utils.observer import Observer
from codepack.messengers.receivable import Receivable
from codepack.messengers.sendable import Sendable
from codepack.utils.double_key_map import DoubleKeyMap


class Supervisor(JobManager):
    def __init__(self,
                 topic: str,
                 sender: Sendable,
                 receiver: Receivable,
                 background: bool = True) -> None:
        self.observable = Observable()
        super().__init__(topic=topic,
                         sender=sender,
                         receiver=receiver,
                         background=background)

    def submit_code(self, code: Code, arg: Optional[Arg] = None) -> str:
        code_snapshot = CodeSnapshot(function=code.function, name=code.get_id(), arg=arg)
        code_snapshot.save()
        job = Job(code_snapshot=code_snapshot.get_id(), on_transition=self.notify)
        job.update_state(States.NEW)
        return code_snapshot.serial_number

    def submit_codepack(self, codepack: CodePack, argpack: Optional[ArgPack] = None) -> str:
        code_snapshots = dict()
        links = DoubleKeyMap()
        for code in codepack:
            code_snapshot = CodeSnapshot(function=code.function, name=code.get_id(),
                                         arg=argpack.get_arg_by_code_id(code.get_id()) if argpack else None)
            code_snapshot.save()
            code_snapshots[code.get_id()] = code_snapshot
        for src, dst, param in codepack.links.items():
            links.put(key1=code_snapshots[src].get_id(), key2=code_snapshots[dst].get_id(), value=param)
        if codepack.subscription:
            subscription = code_snapshots[codepack.subscription].get_id()
        else:
            subscription = None
        codepack_snapshot = CodePackSnapshot(code_snapshots=[s.get_id() for s in code_snapshots.values()],
                                             links=links,
                                             subscription=subscription)
        codepack_snapshot.save()
        for serial_number in codepack_snapshot:
            job = Job(code_snapshot=serial_number,
                      codepack_snapshot=codepack_snapshot.get_id(),
                      on_transition=self.notify)
            job.update_state(States.NEW)
        return codepack_snapshot.serial_number

    def get_code_state(self, serial_number: str) -> State:
        return Job.check_state(code_snapshot=serial_number)

    def get_code_result(self, serial_number: str) -> Any:
        return ResultCache.load(id=serial_number).get_data()

    def get_codepack_state(self, serial_number: str) -> State:
        codepack_snapshot = CodePackSnapshot.load(id=serial_number)
        states = list()
        for code_snapshot in codepack_snapshot:
            job = Job.check_state(code_snapshot=code_snapshot)
            states.append(job)
        return States.get_final_state(states=states)

    def get_codepack_result(self, serial_number: str) -> Any:
        codepack_snapshot = CodePackSnapshot.load(id=serial_number)
        if codepack_snapshot.subscription:
            return ResultCache.load(id=codepack_snapshot.subscription).get_data()
        else:
            return None

    def handle(self, job: Job) -> None:
        self.observable.notify_observers(message=job.to_json())

    def subscribe(self, id: str, observer: Observer):
        self.observable.register_observer(id=id, observer=observer)

    def unsubscribe(self, id: str):
        self.observable.unregister_observer(id)
