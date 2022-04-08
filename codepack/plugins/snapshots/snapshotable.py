import abc
from typing import Any, TypeVar


Snapshot = TypeVar('Snapshot', bound='codepack.plugins.snapshots.snapshot.Snapshot')


class Snapshotable(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def to_snapshot(self, *args: Any, **kwargs: Any) -> Snapshot:
        """convert to snapshot"""

    @classmethod
    @abc.abstractmethod
    def from_snapshot(cls, snapshot: Snapshot) -> 'Snapshotable':
        """create instance from snapshot"""
