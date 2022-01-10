from codepack.storage import Storable
from codepack.snapshot import Snapshotable
import abc


class CodePackBase(Storable, Snapshotable, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self, id=None, serial_number=None):
        Storable.__init__(self, id=id, serial_number=serial_number)
