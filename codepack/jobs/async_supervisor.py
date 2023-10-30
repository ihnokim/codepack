from codepack.jobs.job import Job
from codepack.jobs.async_job import AsyncJob
from codepack.code import Code
from codepack.arg import Arg
from codepack.codepack import CodePack
from codepack.argpack import ArgPack
from codepack.asyncio.mixins.async_job_manager_mixin import AsyncJobManagerMixin
from codepack.jobs.supervisor import Supervisor
from codepack.jobs.async_code_snapshot import AsyncCodeSnapshot
from codepack.jobs.async_codepack_snapshot import AsyncCodePackSnapshot
from codepack.jobs.states import AsyncStates
from codepack.jobs.states.state import State
from codepack.jobs.async_result_cache import AsyncResultCache
from codepack.utils.double_key_map import DoubleKeyMap
from typing import Optional, Any


class AsyncSupervisor(AsyncJobManagerMixin, Supervisor):
    async def handle(self, job: Job) -> None:
        self.observable.notify_observers(message=job.to_json())

    async def submit_code(self, code: Code, arg: Optional[Arg] = None) -> str:
        code_snapshot = AsyncCodeSnapshot(function=code.function, name=code.get_id(), arg=arg)
        await code_snapshot.save()
        job = AsyncJob(code_snapshot=code_snapshot.get_id(), on_transition=self.notify)
        await job.update_state(AsyncStates.NEW)
        return code_snapshot.serial_number

    async def submit_codepack(self, codepack: CodePack, argpack: Optional[ArgPack] = None) -> str:
        code_snapshots = dict()
        links = DoubleKeyMap()
        for code in codepack:
            code_snapshot = AsyncCodeSnapshot(function=code.function, name=code.get_id(),
                                              arg=argpack.get_arg_by_code_id(code.get_id()) if argpack else None)
            await code_snapshot.save()
            code_snapshots[code.get_id()] = code_snapshot
        for src, dst, param in codepack.links.items():
            links.put(key1=code_snapshots[src].get_id(), key2=code_snapshots[dst].get_id(), value=param)
        if codepack.subscription:
            subscription = code_snapshots[codepack.subscription].get_id()
        else:
            subscription = None
        codepack_snapshot = AsyncCodePackSnapshot(code_snapshots=[s.get_id() for s in code_snapshots.values()],
                                                  links=links,
                                                  subscription=subscription)
        await codepack_snapshot.save()
        for serial_number in codepack_snapshot:
            job = AsyncJob(code_snapshot=serial_number,
                           codepack_snapshot=codepack_snapshot.get_id(),
                           on_transition=self.notify)
            await job.update_state(AsyncStates.NEW)
        return codepack_snapshot.serial_number

    async def get_code_state(self, serial_number: str) -> State:
        return await AsyncJob.check_state(code_snapshot=serial_number)

    async def get_code_result(self, serial_number: str) -> Any:
        result_cache = await AsyncResultCache.load(id=serial_number)
        return result_cache.get_data()

    async def get_codepack_state(self, serial_number: str) -> State:
        codepack_snapshot = await AsyncCodePackSnapshot.load(id=serial_number)
        states = list()
        for code_snapshot in codepack_snapshot:
            states.append(await AsyncJob.check_state(code_snapshot=code_snapshot))
        return AsyncStates.get_final_state(states=states)

    async def get_codepack_result(self, serial_number: str) -> Any:
        codepack_snapshot = await AsyncCodePackSnapshot.load(id=serial_number)
        if codepack_snapshot.subscription:
            result_cache = await AsyncResultCache.load(id=codepack_snapshot.subscription)
            return result_cache.get_data()
        else:
            return None
