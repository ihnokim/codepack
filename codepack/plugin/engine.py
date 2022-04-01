from codepack.config.default import Default
import abc
from typing import Callable, Optional, TypeVar


SnapshotService = TypeVar('SnapshotService', bound='codepack.plugin.snapshot_service.SnapshotService')


class Engine(metaclass=abc.ABCMeta):
    def __init__(self, callback: Callable, interval: float = 1, config_path: Optional[str] = None,
                 snapshot_service: Optional[SnapshotService] = None) -> None:
        """initialize instance"""
        self.callback = callback
        self.interval = interval
        self.snapshot_service =\
            snapshot_service if snapshot_service else Default.get_service('code_snapshot', 'snapshot_service',
                                                                          config_path=config_path)

    @abc.abstractmethod
    def start(self) -> None:
        """Start loop"""

    @abc.abstractmethod
    def stop(self) -> None:
        """Stop loop"""

    def work(self) -> None:
        for x in self.snapshot_service.search(key='state', value='WAITING'):
            resolved = True
            for s in self.snapshot_service.load([x['serial_number'] for x in x['dependency']], projection={'state'}):
                if s['state'] != 'TERMINATED':
                    resolved = False
                    break
            if resolved:
                self.callback(x)
