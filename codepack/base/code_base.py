from codepack.storages.storable import Storable
from codepack.plugins.snapshots.snapshotable import Snapshotable
from codepack.base.function import Function
import abc
from typing import Optional, Callable


class CodeBase(Storable, Function, Snapshotable, metaclass=abc.ABCMeta):
    def __init__(self,
                 id: Optional[str] = None,
                 serial_number: Optional[str] = None,
                 function: Optional[Callable] = None,
                 source: Optional[str] = None,
                 context: Optional[dict] = None) -> None:
        Storable.__init__(self, id=id, serial_number=serial_number)
        Function.__init__(self, function=function, source=source, context=context)
        Snapshotable.__init__(self)
