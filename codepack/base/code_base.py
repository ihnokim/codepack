from codepack.storages.storable import Storable
from codepack.plugins.snapshots.snapshotable import Snapshotable
from codepack.base.function import Function
import abc
from typing import Optional, Callable


class CodeBase(Storable, Function, Snapshotable, metaclass=abc.ABCMeta):
    def __init__(self,
                 id: Optional[str] = None,
                 serial_number: Optional[str] = None,
                 version: Optional[str] = None,
                 function: Optional[Callable] = None,
                 source: Optional[str] = None,
                 context: Optional[dict] = None) -> None:
        Function.__init__(self, function=function, source=source, context=context)
        Storable.__init__(self, id=id if id is not None else self.function.__name__,
                          serial_number=serial_number,
                          version=version)
        Snapshotable.__init__(self)
