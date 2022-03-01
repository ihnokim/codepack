import abc
from codepack.service.service import Service
from codepack.snapshot import Snapshot


class SnapshotService(Service, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def save(self, snapshot: Snapshot):
        """save snapshot"""

    @abc.abstractmethod
    def load(self, serial_number, projection=None):
        """load snapshot"""

    @abc.abstractmethod
    def remove(self, serial_number):
        """remove snapshot"""

    @abc.abstractmethod
    def search(self, key, value, projection=None):
        """search by key and value"""
