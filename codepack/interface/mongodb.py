from pymongo import MongoClient
from codepack.interface.abc import Interface


class MongoDB(Interface):
    def __init__(self, config, ssh_config=None, **kwargs):
        super().__init__()
        self.client = None
        self.connect(config=config, ssh_config=ssh_config, **kwargs)

    def connect(self, config, ssh_config=None, **kwargs):
        self.config = config
        host, port = self.set_sshtunnel(host=self.config['host'], port=self.config['port'], ssh_config=ssh_config)
        _config = self.exclude_keys(self.config, keys=['host', 'port'])
        if 'connect' in _config:
            _config['connect'] = self.eval_bool(_config['connect'])
        self.client = MongoClient(host=host, port=port, **_config, **kwargs)
        self.closed = False
        return self.client

    def __getitem__(self, item):
        assert not self.closed, "Connection is closed."
        return self.client[item]

    def __getattr__(self, item):
        return self.__getitem__(item)

    def close(self):
        self.client.close()
        if not self.closed:
            if self.ssh_config and self.ssh is not None:
                self.ssh.stop()
                self.ssh = None
            self.closed = True
