from codepack.plugin.engine import Engine
from codepack.utils.looper import Looper
from typing import Callable, Optional, TypeVar


SnapshotService = TypeVar('SnapshotService', bound='codepack.plugin.snapshot_service.SnapshotService')


class NonBlockingEngine(Engine):
    def __init__(self, callback: Callable, interval: float = 1, daemon: bool = True, config_path: Optional[str] = None,
                 snapshot_service: Optional[SnapshotService] = None) -> None:
        super().__init__(callback=callback, interval=interval,
                         config_path=config_path, snapshot_service=snapshot_service)
        self.worker = Looper(self.work, interval=interval, daemon=daemon)

    def start(self) -> None:
        self.worker.start()

    def stop(self) -> None:
        self.worker.stop()
