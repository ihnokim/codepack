from codepack import Code
from codepack.snapshot import CodeSnapshot


class Worker:
    def __init__(self, listener, interval=1):
        self.listener = listener
        self.interval = interval

    def start(self):
        self.listener.consume(self.work, timeout_ms=int(self.interval * 1000))

    def stop(self):
        self.listener.close()

    @staticmethod
    def work(buffer):
        for tp, msgs in buffer.items():
            for msg in msgs:
                code = None
                try:
                    snapshot = CodeSnapshot.from_dict(msg.value)
                    code = Code.from_snapshot(snapshot)
                    code(*snapshot.args, **snapshot.kwargs)
                except Exception as e:
                    print(e)  # log.error(e)
                    if code is not None:
                        code.update_state('ERROR')
                    continue
