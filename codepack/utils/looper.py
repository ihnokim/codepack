import threading
import time
from typing import Any, Callable


class Looper:
    def __init__(self, func: Callable, interval: float = 1, background: bool = True, daemon: bool = True,
                 *args: Any, **kwargs: Any) -> None:
        super().__init__()
        self.func = None
        self.interval = None
        self.running = False
        self.thread = None
        self.daemon = daemon
        self.background = background
        self.set_interval(interval=interval)
        self.args = args
        self.kwargs = kwargs
        self.set_func(func=func)

    def make_thread(self, func: Callable, *args: Any, **kwargs: Any) -> threading.Thread:
        self.thread = threading.Thread(target=self.loop, args=(func, *args), kwargs=kwargs, daemon=self.daemon)
        return self.thread

    def set_interval(self, interval: float) -> None:
        self.interval = interval

    def set_func(self, func: Callable) -> None:
        self.func = func

    def loop(self, func: Callable, *args: Any, **kwargs: Any) -> None:
        try:
            while self.running:
                try:
                    if self.interval > 0:
                        time.sleep(self.interval)
                    func(*args, **kwargs)
                except KeyboardInterrupt:
                    break
        except Exception as e:
            raise e
        finally:
            self.running = False

    def is_alive(self) -> bool:
        if self.background:
            return self.thread.is_alive() if self.thread else False
        else:
            return False

    def is_running(self) -> bool:
        return self.running

    def start(self) -> None:
        if self.background and (self.is_running() or self.is_alive()):
            raise Exception('Thread is already running')
        self.running = True
        if self.background:
            self.make_thread(func=self.func, *self.args, **self.kwargs)
            self.thread.start()
        else:
            self.loop(func=self.func, *self.args, **self.kwargs)

    def restart(self) -> None:
        if self.background and (self.is_running() or self.is_alive()):
            self.stop()
        self.start()

    def stop(self) -> None:
        self.running = False
        if self.background:
            self.thread.join()
