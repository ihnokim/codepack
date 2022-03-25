from codepack.interface.interface import Interface
from pymongo import MongoClient


class MongoDB(Interface):
    def __init__(self, config, *args, **kwargs):
        super().__init__(config)
        self.connect(*args, **kwargs)

    def connect(self, *args, **kwargs):
        host, port = self.bind(host=self.config['host'], port=self.config['port'])
        _config = self.exclude_keys(self.config, keys=['host', 'port'])
        if 'connect' in _config:
            _config['connect'] = self.eval_bool(_config['connect'])
        self.session = MongoClient(host=host, port=port, *args, **_config, **kwargs)
        self._closed = False
        return self.session

    def __getitem__(self, item):
        assert not self.closed(), "connection is closed"
        return self.session[item]

    def __getattr__(self, item):
        assert not self.closed(), "connection is closed"
        return self.__getitem__(item)

    def close(self):
        self.session.close()
        if not self.closed():
            if self.ssh_config and self.ssh is not None:
                self.ssh.stop()
                self.ssh = None
            self._closed = True
