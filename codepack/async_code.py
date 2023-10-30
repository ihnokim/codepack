from codepack.asyncio.mixins.async_item_mixin import AsyncItemMixin
from codepack.code import Code
from codepack.function import Function
from codepack.async_function import AsyncFunction
from typing import Any, Callable, Union, Dict


class AsyncCode(AsyncItemMixin, Code):
    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return await self.function(*args, **kwargs)

    @classmethod
    def _get_function_object(cls, function: Union[Callable, Dict[str, Any]]) -> Function:
        if isinstance(function, Function):
            return function
        elif isinstance(function, dict):
            return AsyncFunction.from_dict(d=function)
        else:
            return AsyncFunction(function=function)
