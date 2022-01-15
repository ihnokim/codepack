from docker import DockerClient
from codepack.interface import Interface


class Docker(Interface):
    def __init__(self, config=None, *args, **kwargs):
        if not config:
            config = dict()
        super().__init__(config)
        self.connect(*args, **kwargs)

    def connect(self, *args, **kwargs):
        self.session = DockerClient(*args, **self.config, **kwargs)
        self.closed = False
        return self.session

    def __getattr__(self, item):
        assert not self.closed, "connection is closed"
        return getattr(self.session, item)

    def close(self):
        self.session.close()
        self.closed = True
