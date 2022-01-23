from codepack import Code
from codepack.snapshot import CodeSnapshot
from codepack.interface import KafkaConsumer
from codepack.utils.config import get_default_service_config


class Worker:
    def __init__(self, consumer=None, interval=None, callback=None, config_path=None):
        self.consumer = consumer
        self.interval = interval
        if self.consumer is None:
            config = get_default_service_config('worker', config_path=config_path)
            consumer_config = config['kafka']
            for k, v in config.items():
                if k not in ['kafka', 'interval', 'source']:
                    consumer_config[k] = v
            self.consumer = KafkaConsumer(consumer_config)
            if self.interval is None:
                self.interval = config.get('interval', 1)
        self.callback = None
        self.register(callback=callback)

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
