import uuid
from datetime import datetime
from typing import Any, Dict, Optional, Callable, List, Tuple
from codepack.jobs.code_snapshot import CodeSnapshot
from codepack.jobs.codepack_snapshot import CodePackSnapshot
from codepack.item import Item
from codepack.jobs.states.state import State
from codepack.jobs.states import States
from codepack.jobs.result_cache import ResultCache
from codepack.jobs.lock import Lock


class Job(Item):
    def __init__(self,
                 code_snapshot: str,
                 codepack_snapshot: Optional[str] = None,
                 id: Optional[str] = None,
                 state: State = States.UNKNOWN,
                 timestamp: Optional[float] = None,
                 on_transition: Optional[Callable[['Job'], None]] = None) -> None:
        self.code_snapshot = code_snapshot
        self.codepack_snapshot = codepack_snapshot
        self._code_snapshot = None
        self._codepack_snapshot = None
        self.state = state
        self.timestamp = timestamp if timestamp else datetime.now().timestamp()
        self.id: str = id if id else str(uuid.uuid4())
        self.on_transition = on_transition
        super().__init__()

    def get_id(self) -> str:
        return self.id

    def __serialize__(self) -> Dict[str, Any]:
        return {'id': self.id,
                'code_snapshot': self.code_snapshot,
                'codepack_snapshot': self.codepack_snapshot,
                'state': self.state.get_name(),
                'timestamp': self.timestamp}

    @classmethod
    def __deserialize__(cls, d: Dict[str, Any]) -> Item:
        # Requestor 추가 필요
        return cls(id=d['id'],
                   code_snapshot=d['code_snapshot'],
                   codepack_snapshot=d['codepack_snapshot'],
                   state=getattr(States, d['state']),
                   timestamp=d['timestamp'])

    def get_code_snapshot(self) -> CodeSnapshot:
        if not self._code_snapshot:
            self._code_snapshot = CodeSnapshot.load(id=self.code_snapshot)
        return self._code_snapshot

    def get_codepack_snapshot(self) -> Optional[CodePackSnapshot]:
        if self.codepack_snapshot is None:
            return None
        if not self._codepack_snapshot:
            self._codepack_snapshot = CodePackSnapshot.load(id=self.codepack_snapshot)
        return self._codepack_snapshot

    def set_callback(self, on_transition: Optional[Callable[['Job'], None]] = None) -> None:
        self.on_transition = on_transition

    def update_state(self, state: State) -> None:
        self.state = state
        self.id = str(uuid.uuid4())
        self.timestamp = datetime.now().timestamp()
        if self.on_transition:
            self.on_transition(self)

    def get_upstream_states(self) -> List[State]:
        states = list()
        codepack_snapshot = self.get_codepack_snapshot()
        if codepack_snapshot is None:
            return list()
        upstream = codepack_snapshot.get_upstream(key=self.code_snapshot)
        for serial_number in upstream:
            states.append(self.check_state(code_snapshot=serial_number))
        return states

    @classmethod
    def history(cls, code_snapshot: str) -> List[Tuple[State, float]]:
        storage = cls.get_storage()
        job_ids = storage.search(key='code_snapshot', value=code_snapshot)
        job_dicts = storage.load_many(id=job_ids)
        return sorted([(x['state'], x['timestamp']) for x in job_dicts], key=lambda x: x[1])

    @classmethod
    def check_state(cls, code_snapshot: str) -> State:
        states = cls.history(code_snapshot=code_snapshot)
        return getattr(States, states[-1][0]) if states else States.UNKNOWN

    def get_downstream_jobs(self) -> List['Job']:
        jobs = list()
        codepack_snapshot = self.get_codepack_snapshot()
        if codepack_snapshot is None:
            return list()
        downstream = codepack_snapshot.get_downstream(key=self.code_snapshot)
        for serial_number in downstream:
            jobs.append(Job(code_snapshot=serial_number,
                            codepack_snapshot=self.codepack_snapshot,
                            on_transition=self.on_transition))
        return jobs

    def check_if_dependencies_resolved(self) -> Dict[str, bool]:
        ret = dict()
        codepack_snapshot = self.get_codepack_snapshot()
        if codepack_snapshot is None:
            return dict()
        dependencies = codepack_snapshot.get_dependencies(key=self.code_snapshot)
        code_snapshots = list(dependencies.values())
        result_cache_storage = ResultCache.get_storage()
        exists = result_cache_storage.exists_many(id=code_snapshots)
        for i, code_snapshot in enumerate(code_snapshots):
            ret[code_snapshot] = exists[i]
        return ret

    def get_required_data(self) -> Dict[str, Any]:
        ret = dict()
        codepack_snapshot = self.get_codepack_snapshot()
        if codepack_snapshot is None:
            return dict()
        dependencies = codepack_snapshot.get_dependencies(key=self.code_snapshot)
        for param, src in dependencies.items():
            result_cache = ResultCache.load(src)
            ret[param] = result_cache.get_data()
        return ret

    def is_already_executed(self) -> bool:
        lock = Lock(key=self.code_snapshot)
        is_lock_retrieved = lock.retrieve()
        return not is_lock_retrieved

    def propagate_down(self, state: State) -> None:
        jobs = self.get_downstream_jobs()
        for job in jobs:
            job.update_state(state=state)

    def handle(self) -> None:
        self.state.handle(context=self)
