from codepack.engine.engine import Engine
import time


class BlockingEngine(Engine):
    def __init__(self, callback, interval=1, config_path=None, snapshot_service=None):
        super().__init__(callback=callback, interval=interval, config_path=config_path, snapshot_service=snapshot_service)

    def start(self):
        while True:
            try:
                time.sleep(self.interval)
                self.work()
            except KeyboardInterrupt:
                break
            finally:
                self.stop()

    def stop(self):
        pass
