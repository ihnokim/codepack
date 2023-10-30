from codepack.jobs.states.ready_state import ReadyState
from codepack.asyncio.mixins.async_state_mixin import AsyncStateMixin
from codepack.jobs.async_result_cache import AsyncResultCache
from codepack.jobs.async_code_snapshot import AsyncCodeSnapshot
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from codepack.jobs.job import Job  # pragma: no cover


class AsyncReadyState(AsyncStateMixin, ReadyState):
    async def handle(self, context: 'Job') -> None:
        from codepack.jobs.states import AsyncStates
        try:
            if not await context.is_already_executed():
                code_snapshot: AsyncCodeSnapshot = await context.get_code_snapshot()
                await context.update_state(state=AsyncStates.RUNNING)
                required_data = await context.get_required_data()
                if code_snapshot.arg:
                    ret = await code_snapshot.function(*code_snapshot.arg.args,
                                                       **code_snapshot.arg.kwargs,
                                                       **required_data)
                else:
                    ret = await code_snapshot.function(**required_data)
                result = AsyncResultCache(data=ret, serial_number=code_snapshot.serial_number, name=code_snapshot.name)
                is_saved = await result.save()
                if is_saved:
                    await context.update_state(state=AsyncStates.TERMINATED)
                else:
                    await context.update_state(state=AsyncStates.ERROR)
        except Exception as e:
            print(e)
            await context.update_state(state=AsyncStates.ERROR)  # TODO: add error message e
