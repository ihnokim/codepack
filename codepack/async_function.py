from codepack.function import Function
import asyncio
from typing import Any


class AsyncFunction(Function):
    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        for k, v in self.context.items():
            if k not in kwargs:
                kwargs[k] = v
        if asyncio.iscoroutinefunction(self.function):
            return await self.function(*args, **kwargs)
        else:
            return self.function(*args, **kwargs)
