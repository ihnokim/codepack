from codepack.utils.config.default import Default
from codepack.utils.looper import Looper
from typing import Callable, Optional, TypeVar


SnapshotService = TypeVar('SnapshotService', bound='codepack.plugins.snapshots.snapshot_service.SnapshotService')


class DependencyMonitor:
    def __init__(self, callback: Callable, interval: float = 10, background: bool = False,
                 daemon: bool = True, config_path: Optional[str] = None,
                 snapshot_service: Optional[SnapshotService] = None) -> None:
        self.callback = callback
        self.interval = interval
        self.snapshot_service =\
            snapshot_service if snapshot_service else Default.get_service('code_snapshot', 'snapshot_service',
                                                                          config_path=config_path)
        self.looper = Looper(self.monitor, interval=interval, background=background, daemon=daemon)

    def start(self) -> None:
        self.looper.start()

    def stop(self) -> None:
        self.looper.stop()

    def monitor(self) -> None:
        for x in self.snapshot_service.search(key='state', value='WAITING'):
            resolved = True
            for s in self.snapshot_service.load([x['serial_number'] for x in x['dependency']], projection={'state'}):
                if s['state'] != 'TERMINATED':
                    resolved = False
                    break
            if resolved:
                self.callback(x)
