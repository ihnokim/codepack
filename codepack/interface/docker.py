from docker import DockerClient
from codepack.interface import Interface


class Docker(Interface):
    def __init__(self, config=None, *args, **kwargs):
        super().__init__(config if config else dict())
        self.connect(*args, **kwargs)

    def connect(self, *args, **kwargs):
        self.session = DockerClient(*args, **self.config, **kwargs)
        self._closed = False
        return self.session

    def __getattr__(self, item):
        assert not self.closed(), "connection is closed"
        return getattr(self.session, item)

    def close(self):
        self.session.close()
        if not self.closed():
            self._closed = True
