from codepack.plugin.engine import Engine
import time
from typing import Callable, Optional, TypeVar


SnapshotService = TypeVar('SnapshotService', bound='codepack.plugin.snapshot_service.SnapshotService')


class BlockingEngine(Engine):
    def __init__(self, callback: Callable, interval: float = 1, config_path: Optional[str] = None,
                 snapshot_service: Optional[SnapshotService] = None) -> None:
        super().__init__(callback=callback, interval=interval, config_path=config_path,
                         snapshot_service=snapshot_service)

    def start(self) -> None:
        while True:
            try:
                time.sleep(self.interval)
                self.work()
            except KeyboardInterrupt:
                break
            finally:
                self.stop()

    def stop(self) -> None:
        pass
