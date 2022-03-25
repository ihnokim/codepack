from codepack.storage.storable import Storable
from codepack.snapshot.snapshotable import Snapshotable
import abc


class CodePackBase(Storable, Snapshotable, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self, id: str = None, serial_number: str = None):
        Storable.__init__(self, id=id, serial_number=serial_number)
