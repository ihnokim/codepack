import abc


class Snapshotable(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def to_snapshot(self, *args, **kwargs):
        """convert to snapshot"""

    @classmethod
    @abc.abstractmethod
    def from_snapshot(cls, snapshot):
        """create instance from snapshot"""
