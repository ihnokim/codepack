from codepack.storages.storable import Storable
from codepack.plugins.snapshots.snapshotable import Snapshotable
import abc
from typing import Optional


class CodePackBase(Storable, Snapshotable, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self, name: str,
                 serial_number: Optional[str] = None,
                 version: Optional[str] = None,
                 timestamp: Optional[float] = None) -> None:
        Storable.__init__(self, name=name, serial_number=serial_number, version=version,
                          timestamp=timestamp, id_key='_name')
