from codepack.jobs.states.running_state import RunningState
from codepack.asyncio.mixins.async_state_mixin import AsyncStateMixin


class AsyncRunningState(AsyncStateMixin, RunningState):
    pass
