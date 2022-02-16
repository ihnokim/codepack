import requests
from codepack import Code
from codepack.snapshot import CodeSnapshot
from codepack.employee.supervisor import Supervisor
from codepack.storage import KafkaStorage


class Worker(KafkaStorage):
    def __init__(self, consumer=None, interval=1, callback=None, supervisor=None, consumer_config=None):
        KafkaStorage.__init__(self, consumer=consumer, consumer_config=consumer_config)
        self.interval = interval
        self.supervisor = supervisor
        self.callback = callback
        if self.supervisor and not self.callback:
            if isinstance(self.supervisor, str) or isinstance(self.supervisor, Supervisor):
                self.callback = self.inform_supervisor_of_termination
            else:
                self.close()
                raise TypeError(type(self.supervisor))
        self.register(callback=self.callback)

    def register(self, callback):
        self.callback = callback

    def start(self):
        self.consumer.consume(self.work, timeout_ms=int(float(self.interval) * 1000))

    def stop(self):
        pass

    def work(self, buffer):
        for tp, msgs in buffer.items():
            for msg in msgs:
                code = None
                try:
                    snapshot = CodeSnapshot.from_dict(msg.value)
                    code = Code.from_snapshot(snapshot)
                    code.register(callback=self.callback)
                    code(*snapshot.args, **snapshot.kwargs)
                except Exception as e:
                    print(e)  # log.error(e)
                    if code is not None:
                        code.update_state('ERROR')
                    continue

    def inform_supervisor_of_termination(self, x):
        if x['state'] == 'TERMINATED':
            if isinstance(self.supervisor, str):
                requests.get(self.supervisor + '/organize/%s' % x['serial_number'])
            elif isinstance(self.supervisor, Supervisor):
                self.supervisor.organize(x['serial_number'])
