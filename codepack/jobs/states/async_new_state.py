from codepack.jobs.states.new_state import NewState
from codepack.asyncio.mixins.async_state_mixin import AsyncStateMixin
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from codepack.jobs.job import Job  # pragma: no cover


class AsyncNewState(AsyncStateMixin, NewState):
    async def handle(self, context: 'Job') -> None:
        from codepack.jobs.states import AsyncStates
        await context.update_state(state=AsyncStates.CHECKING)
