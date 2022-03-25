from codepack.engine.engine import Engine
from codepack.utils.looper import Looper


class NonBlockingEngine(Engine):
    def __init__(self, callback, interval=1, daemon=True, config_path=None, snapshot_service=None):
        super().__init__(callback=callback, interval=interval, config_path=config_path, snapshot_service=snapshot_service)
        self.worker = Looper(self.work, interval=interval, daemon=daemon)

    def start(self):
        self.worker.start()

    def stop(self):
        self.worker.stop()
