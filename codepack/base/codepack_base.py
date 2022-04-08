from codepack.storages.storable import Storable
from codepack.plugins.snapshots.snapshotable import Snapshotable
import abc
from typing import Optional


class CodePackBase(Storable, Snapshotable, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self, id: Optional[str] = None, serial_number: Optional[str] = None) -> None:
        Storable.__init__(self, id=id, serial_number=serial_number)
