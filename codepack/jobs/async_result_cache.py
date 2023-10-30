from codepack.jobs.result_cache import ResultCache
from codepack.asyncio.mixins.async_item_mixin import AsyncItemMixin


class AsyncResultCache(AsyncItemMixin, ResultCache):
    pass
