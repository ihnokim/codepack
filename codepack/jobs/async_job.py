import uuid
from datetime import datetime
from codepack.asyncio.mixins.async_item_mixin import AsyncItemMixin
from codepack.jobs.states import AsyncStates
from codepack.jobs.states.state import State
from codepack.jobs.job import Job
from codepack.item import Item
from typing import Optional, List, Tuple, Dict, Any
from codepack.jobs.async_lock import AsyncLock
from codepack.jobs.async_result_cache import AsyncResultCache
from codepack.jobs.async_code_snapshot import AsyncCodeSnapshot
from codepack.jobs.async_codepack_snapshot import AsyncCodePackSnapshot


class AsyncJob(AsyncItemMixin, Job):
    async def get_code_snapshot(self) -> AsyncCodeSnapshot:
        if not self._code_snapshot:
            self._code_snapshot = await AsyncCodeSnapshot.load(id=self.code_snapshot)
        return self._code_snapshot

    async def get_codepack_snapshot(self) -> Optional[AsyncCodePackSnapshot]:
        if self.codepack_snapshot is None:
            return None
        if not self._codepack_snapshot:
            self._codepack_snapshot = await AsyncCodePackSnapshot.load(id=self.codepack_snapshot)
        return self._codepack_snapshot

    async def update_state(self, state: State) -> None:
        self.state = state
        self.id = str(uuid.uuid4())
        self.timestamp = datetime.now().timestamp()
        if self.on_transition:
            await self.on_transition(self)

    async def get_upstream_states(self) -> List[State]:
        states = list()
        codepack_snapshot = await self.get_codepack_snapshot()
        if codepack_snapshot is None:
            return list()
        upstream = codepack_snapshot.get_upstream(key=self.code_snapshot)
        for serial_number in upstream:
            states.append(await self.check_state(code_snapshot=serial_number))
        return states

    @classmethod
    async def history(cls, code_snapshot: str) -> List[Tuple[State, float]]:
        storage = cls.get_storage()
        job_ids = await storage.search(key='code_snapshot', value=code_snapshot)
        job_dicts = await storage.load_many(id=job_ids)
        return sorted([(x['state'], x['timestamp']) for x in job_dicts], key=lambda x: x[1])

    @classmethod
    async def check_state(cls, code_snapshot: str) -> State:
        states = await cls.history(code_snapshot=code_snapshot)
        return getattr(AsyncStates, states[-1][0]) if states else AsyncStates.UNKNOWN

    async def get_downstream_jobs(self) -> List['Job']:
        jobs = list()
        codepack_snapshot = await self.get_codepack_snapshot()
        if codepack_snapshot is None:
            return list()
        downstream = codepack_snapshot.get_downstream(key=self.code_snapshot)
        for serial_number in downstream:
            jobs.append(AsyncJob(code_snapshot=serial_number,
                                 codepack_snapshot=self.codepack_snapshot,
                                 on_transition=self.on_transition))
        return jobs

    async def check_if_dependencies_resolved(self) -> Dict[str, bool]:
        ret = dict()
        codepack_snapshot = await self.get_codepack_snapshot()
        if codepack_snapshot is None:
            return dict()
        dependencies = codepack_snapshot.get_dependencies(key=self.code_snapshot)
        code_snapshots = list(dependencies.values())
        result_cache_storage = AsyncResultCache.get_storage()
        exists = await result_cache_storage.exists_many(id=code_snapshots)
        for i, code_snapshot in enumerate(code_snapshots):
            ret[code_snapshot] = exists[i]
        return ret

    async def get_required_data(self) -> Dict[str, Any]:
        ret = dict()
        codepack_snapshot = await self.get_codepack_snapshot()
        if codepack_snapshot is None:
            return dict()
        dependencies = codepack_snapshot.get_dependencies(key=self.code_snapshot)
        for param, src in dependencies.items():
            result_cache = await AsyncResultCache.load(src)
            ret[param] = result_cache.get_data()
        return ret

    async def is_already_executed(self) -> bool:
        lock = AsyncLock(key=self.code_snapshot)
        is_lock_retrieved = await lock.retrieve()
        return not is_lock_retrieved

    async def propagate_down(self, state: State) -> None:
        jobs = await self.get_downstream_jobs()
        for job in jobs:
            await job.update_state(state=state)

    async def handle(self) -> None:
        await self.state.handle(context=self)

    @classmethod
    def __deserialize__(cls, d: Dict[str, Any]) -> Item:
        # Requestor 추가 필요
        return cls(id=d['id'],
                   code_snapshot=d['code_snapshot'],
                   codepack_snapshot=d['codepack_snapshot'],
                   state=getattr(AsyncStates, d['state']),
                   timestamp=d['timestamp'])
