from codepack.jobs.states.checking_state import CheckingState
from codepack.asyncio.mixins.async_state_mixin import AsyncStateMixin
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from codepack.jobs.job import Job  # pragma: no cover


class AsyncCheckingState(AsyncStateMixin, CheckingState):
    async def handle(self, context: 'Job') -> None:
        from codepack.jobs.states import AsyncStates
        upstream_states = await context.get_upstream_states()
        upstream_state = AsyncStates.TERMINATED
        if upstream_states:
            for state in upstream_states:
                if state == AsyncStates.TERMINATED:
                    continue
                elif state == AsyncStates.ERROR:
                    upstream_state = AsyncStates.ERROR
                    break
                else:
                    upstream_state = AsyncStates.WAITING
                    break
            if upstream_state != AsyncStates.TERMINATED:
                await context.update_state(state=upstream_state)
                return
            dependency_resolved = await context.check_if_dependencies_resolved()
            for snapshot, resolved in dependency_resolved.items():
                if not resolved:
                    await context.update_state(state=AsyncStates.ERROR)
                    return
        await context.update_state(state=AsyncStates.READY)
