from typing import Any, Dict
from codepack.asyncio.mixins.async_item_mixin import AsyncItemMixin
from codepack.jobs.code_snapshot import CodeSnapshot
from codepack.async_function import AsyncFunction
from codepack.arg import Arg


class AsyncCodeSnapshot(AsyncItemMixin, CodeSnapshot):
    @classmethod
    def __deserialize__(cls, d: Dict[str, Any]) -> 'CodeSnapshot':
        return cls(serial_number=d['serial_number'],
                   name=d['name'],
                   function=AsyncFunction.from_dict(d['function']),
                   arg=Arg.from_dict(d['arg']) if d['arg'] else None,
                   timestamp=d['timestamp'])
