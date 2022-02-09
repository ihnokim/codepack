from kafka import KafkaConsumer as Kafka
from codepack.interface import Interface
from copy import deepcopy
import json


class KafkaConsumer(Interface):
    def __init__(self, config, *args, **kwargs):
        super().__init__(config)
        self.connect(*args, **kwargs)

    def connect(self, *args, **kwargs):
        if 'value_deserializer' not in self.config and 'value_deserializer' not in kwargs:
            kwargs['value_deserializer'] = lambda x: json.loads(x.decode('utf-8'))
        if 'topic' in self.config:
            _config = deepcopy(self.config)
            _topic = _config.pop('topic')
            self.session = Kafka(_topic, *args, **_config, **kwargs)
        else:
            self.session = Kafka(*args, **self.config, **kwargs)
        self._closed = False
        return self.session

    def __getattr__(self, item):
        assert not self.closed(), "connection is closed"
        return getattr(self.session, item)

    def consume(self, callback, *args, **kwargs):
        while True:
            try:
                buffer = self.session.poll(*args, **kwargs)
                if buffer:
                    callback(buffer)
            except KeyboardInterrupt:
                break

    def close(self):
        self.session.close()
        if not self.closed():
            if self.ssh_config and self.ssh is not None:
                self.ssh.stop()
                self.ssh = None
            self._closed = True
