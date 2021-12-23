from codepack.engine.abc import Engine
from codepack.utils import Looper


class NonBlockingEngine(Engine):
    def __init__(self, callback, interval=1, daemon=True, config_path=None, state_manager=None):
        super().__init__(callback=callback, interval=interval, config_path=config_path, state_manager=state_manager)
        self.worker = Looper(self.work, interval=interval, daemon=daemon)

    def start(self):
        self.worker.start()

    def stop(self):
        self.worker.stop()
