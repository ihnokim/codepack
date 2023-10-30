from codepack.jobs.lock import Lock
from codepack.asyncio.mixins.async_item_mixin import AsyncItemMixin


class AsyncLock(AsyncItemMixin, Lock):
    async def retrieve(self) -> bool:
        return await self.save()
