from codepack.jobs.states.waiting_state import WaitingState
from codepack.asyncio.mixins.async_state_mixin import AsyncStateMixin


class AsyncWaitingState(AsyncStateMixin, WaitingState):
    pass
