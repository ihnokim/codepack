import requests
from codepack import Code
from codepack.snapshot import CodeSnapshot
from codepack.interface import KafkaConsumer
from codepack.config import Config
from codepack.employee.supervisor import Supervisor


class Worker:
    def __init__(self, consumer=None, interval=None, callback=None, supervisor=None, config_path=None):
        self.consumer = consumer
        self.interval = interval
        self.supervisor = supervisor
        self.callback = callback
        new_consumer = False
        if self.consumer is None:
            config = Config(config_path=config_path)
            storage_config = config.get_storage_config(section='worker')
            consumer_config = storage_config['kafka']
            for k, v in storage_config.items():
                if k not in ['kafka', 'interval', 'source', 'supervisor']:
                    consumer_config[k] = v
            self.consumer = KafkaConsumer(consumer_config)
            new_consumer = True
            if self.interval is None:
                self.interval = storage_config.get('interval', 1)
            if self.supervisor is None:
                self.supervisor = storage_config.get('supervisor', None)
        if self.supervisor and not self.callback:
            if isinstance(self.supervisor, str) or isinstance(self.supervisor, Supervisor):
                self.callback = self.inform_supervisor_of_termination
            else:
                if new_consumer:
                    self.consumer.close()
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
