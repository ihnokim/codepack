from codepack.interfaces.interface import Interface
import docker
from typing import Any, Optional


class Docker(Interface):
    def __init__(self, config: Optional[dict] = None, *args: Any, **kwargs: Any) -> None:
        super().__init__(config if config else dict())
        self.connect(*args, **kwargs)

    def connect(self, *args: Any, **kwargs: Any) -> docker.DockerClient:
        _config = {k: v for k, v in self.config.items()}
        for k, v in kwargs.items():
            _config[k] = v
        self.session = docker.DockerClient(*args, **_config)
        self._closed = False
        return self.session

    def __getattr__(self, item: str) -> Any:
        assert not self.closed(), "connection is closed"
        return getattr(self.session, item)

    def close(self) -> None:
        self.session.close()
        if not self.closed():
            self._closed = True
