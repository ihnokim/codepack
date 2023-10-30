from codepack.asyncio.mixins.async_item_mixin import AsyncItemMixin
from codepack.codepack import CodePack
from codepack.argpack import ArgPack
from queue import Queue
from typing import Any, Optional, Set


class AsyncCodePack(AsyncItemMixin, CodePack):
    async def __call__(self, argpack: Optional[ArgPack] = None) -> Any:
        results = dict()
        executed: Set[str] = set()
        q = Queue()
        for code in self._get_roots():
            q.put(code)
        while not q.empty():
            code = q.get()
            if code.get_id() in executed:
                continue
            dependencies = {param: results[src.get_id()] for param, src in code.dependencies.items()}
            if argpack:
                arg = argpack.get_arg_by_code_id(code_id=code.get_id())
                if arg:
                    results[code.get_id()] = await code(*arg.args, **arg.kwargs, **dependencies)
                else:
                    results[code.get_id()] = await code(**dependencies)
            else:
                results[code.get_id()] = await code(**dependencies)
            executed.add(code.get_id())
            for child in code.downstream:
                q.put(child)
        if self.subscription:
            return results[self.subscription]
        else:
            return None
