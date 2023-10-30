from codepack.jobs.states.unknown_state import UnknownState
from codepack.asyncio.mixins.async_state_mixin import AsyncStateMixin


class AsyncUnknownState(AsyncStateMixin, UnknownState):
    pass
