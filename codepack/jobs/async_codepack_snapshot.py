from codepack.asyncio.mixins.async_item_mixin import AsyncItemMixin
from codepack.jobs.codepack_snapshot import CodePackSnapshot


class AsyncCodePackSnapshot(AsyncItemMixin, CodePackSnapshot):
    pass
