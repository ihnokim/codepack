from codepack.interfaces.interface import Interface
from codepack.utils.looper import Looper
import kafka
from copy import deepcopy
import json
from typing import Any, Optional, Callable


class KafkaConsumer(Interface):
    def __init__(self, config: dict, *args: Any, **kwargs: Any) -> None:
        super().__init__(config)
        self.connect(*args, **kwargs)
        self.looper = None

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

    def consume(self, callback: Callable, background: bool = False, *args: Any, **kwargs: Any) -> None:
        assert not self.closed(), "connection is closed"
        self.stop()
        self.looper = Looper(func=self.fetch, interval=0, background=background, callback=callback, *args, **kwargs)
        self.looper.start()

    def stop(self):
        if self.looper:
            self.looper.stop()

    def fetch(self, callback: Callable, *args: Any, **kwargs: Any) -> Optional[Any]:
        buffer = self.session.poll(*args, **kwargs)
        if buffer:
            return callback(buffer)
        else:
            return None

    def close(self) -> None:
        self.stop()
        self.session.close()
        if not self.closed():
            if self.ssh_config and self.ssh is not None:
                self.ssh.stop()
                self.ssh = None
            self._closed = True
