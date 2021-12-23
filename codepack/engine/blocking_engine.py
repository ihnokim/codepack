from codepack.engine.abc import Engine
import time


class BlockingEngine(Engine):
    def __init__(self, callback, interval=1, config_path=None, state_manager=None):
        super().__init__(callback=callback, interval=interval, config_path=config_path, state_manager=state_manager)

    def start(self):
        while True:
            try:
                time.sleep(self.interval)
                self.work()
            except KeyboardInterrupt:
                self.stop()
                break

    def stop(self):
        pass
