from pymongo import MongoClient
from codepack.interface.abc import Interface
from sshtunnel import SSHTunnelForwarder


class MongoDB(Interface):
    def __init__(self, config, **kwargs):
        super().__init__(config)
        self.config = None
        self.ssh = None
        self.client = None
        self.closed = True
        self.connect(config, **kwargs)

    def connect(self, config, **kwargs):
        self.config = config
        if self.config['ssh_tunneling'] == 'enable':
            self.ssh = SSHTunnelForwarder((self.config['ssh_host'], int(self.config['ssh_port'])),
                                          ssh_password=self.config['ssh_password'],
                                          ssh_username=self.config['ssh_username'],
                                          remote_bind_address=(self.config['host'], int(self.config['port'])))
            self.ssh.start()
            host = '127.0.0.1'
            port = self.ssh.local_bind_port
        else:
            host = self.config['host']
            port = int(self.config['port'])

        self.client = MongoClient(host=host,
                                  port=port,
                                  username=self.config['username'],
                                  password=self.config['password'],
                                  **kwargs)
        self.closed = False

    def __getitem__(self, item):
        assert not self.closed, "Connection is closed."
        return self.client[item]

    def __getattr__(self, item):
        return self.__getitem__(item)

    def close(self):
        if not self.closed:
            self.client.close()
            if self.config['ssh_tunneling'] == 'enable':
                if self.ssh is not None:
                    self.ssh.stop()
                    self.ssh = None
            self.closed = True

    def __del__(self):
        self.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
