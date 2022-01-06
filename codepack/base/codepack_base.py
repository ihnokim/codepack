from codepack.storage import Storable
import abc


class CodePackBase(Storable, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self, id=None, serial_number=None):
        super().__init__(id=id, serial_number=serial_number)
