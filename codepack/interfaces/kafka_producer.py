from codepack.interfaces.interface import Interface
import kafka
import json
from typing import Any


class KafkaProducer(Interface):
    def __init__(self, config: dict, *args: Any, **kwargs: Any) -> None:
        super().__init__(config)
        self.connect(*args, **kwargs)

    def connect(self, *args: Any, **kwargs: Any) -> kafka.KafkaProducer:
        if 'value_serializer' not in self.config and 'value_serializer' not in kwargs:
            kwargs['value_serializer'] = lambda x: json.dumps(x).encode('utf-8')
        self.session = kafka.KafkaProducer(**self.config, **kwargs)
        self._closed = False
        return self.session

    def __getattr__(self, item: str) -> Any:
        assert not self.closed(), "connection is closed"
        return getattr(self.session, item)

    def produce(self, *args: Any, **kwargs: Any) -> None:
        assert not self.closed(), "connection is closed"
        self.session.send(*args, **kwargs)
        self.session.flush()

    def close(self) -> None:
        self.session.close()
        if not self.closed():
            if self.ssh_config and self.ssh is not None:
                self.ssh.stop()
                self.ssh = None
            self._closed = True
