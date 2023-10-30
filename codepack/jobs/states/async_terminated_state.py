from codepack.jobs.states.terminated_state import TerminatedState
from codepack.asyncio.mixins.async_state_mixin import AsyncStateMixin
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from codepack.jobs.job import Job  # pragma: no cover


class AsyncTerminatedState(AsyncStateMixin, TerminatedState):
    async def handle(self, context: 'Job') -> None:
        from codepack.jobs.states import AsyncStates
        await context.propagate_down(state=AsyncStates.CHECKING)
