from codepack.jobs.states.error_state import ErrorState
from codepack.asyncio.mixins.async_state_mixin import AsyncStateMixin
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from codepack.jobs.job import Job  # pragma: no cover


class AsyncErrorState(AsyncStateMixin, ErrorState):
    async def handle(self, context: 'Job') -> None:
        from codepack.jobs.states import AsyncStates
        await context.propagate_down(state=AsyncStates.ERROR)
