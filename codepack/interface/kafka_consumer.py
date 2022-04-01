from codepack.interface.interface import Interface
import kafka
from copy import deepcopy
import json
from typing import Any, Callable


class KafkaConsumer(Interface):
    def __init__(self, config: dict, *args: Any, **kwargs: Any) -> None:
        super().__init__(config)
        self.connect(*args, **kwargs)

    def connect(self, *args: Any, **kwargs: Any) -> kafka.KafkaConsumer:
        if 'value_deserializer' not in self.config and 'value_deserializer' not in kwargs:
            kwargs['value_deserializer'] = lambda x: json.loads(x.decode('utf-8'))
        if 'topic' in self.config:
            _config = deepcopy(self.config)
            _topic = _config.pop('topic')
            self.session = kafka.KafkaConsumer(_topic, *args, **_config, **kwargs)
        else:
            self.session = kafka.KafkaConsumer(*args, **self.config, **kwargs)
        self._closed = False
        return self.session

    def __getattr__(self, item: str) -> Any:
        assert not self.closed(), "connection is closed"
        return getattr(self.session, item)

    def consume(self, callback: Callable, *args: Any, **kwargs: Any) -> None:
        assert not self.closed(), "connection is closed"
        while True:
            try:
                buffer = self.session.poll(*args, **kwargs)
                if buffer:
                    callback(buffer)
            except KeyboardInterrupt:
                break

    def close(self) -> None:
        self.session.close()
        if not self.closed():
            if self.ssh_config and self.ssh is not None:
                self.ssh.stop()
                self.ssh = None
            self._closed = True
