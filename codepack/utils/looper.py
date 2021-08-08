import threading
import time


class Looper:
    def __init__(self, func, interval=1, daemon=True, *args, **kwargs):
        super().__init__()
        self.func = None
        self.interval = None
        self.running = False
        self.thread = None
        self.daemon = daemon
        self.set_interval(interval=interval)
        self.args = args
        self.kwargs = kwargs
        self.set_func(func=func)

    def make_thread(self, func, *args, **kwargs):
        self.thread = threading.Thread(target=self.loop, args=(func, *args), kwargs=kwargs, daemon=self.daemon)
        return self.thread

    def set_interval(self, interval):
        self.interval = interval

    def set_func(self, func):
        self.func = func

    def loop(self, func, *args, **kwargs):
        try:
            while self.running:
                time.sleep(self.interval)
                func(*args, **kwargs)
        except Exception as e:
            raise e
        finally:
            self.running = False

    def is_alive(self):
        return self.thread.is_alive() if self.thread else False

    def is_running(self):
        return self.running

    def start(self):
        if self.is_running() or self.is_alive():
            raise Exception('Thread is already running')
        self.running = True
        self.make_thread(func=self.func, *self.args, **self.kwargs)
        return self.thread.start()

    def restart(self):
        if self.is_running() or self.is_alive():
            self.stop()
        return self.start()

    def stop(self):
        self.running = False
        return self.thread.join()
