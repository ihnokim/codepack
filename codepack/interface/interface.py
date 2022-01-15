import abc
from sshtunnel import SSHTunnelForwarder
from codepack.utils.config import get_config
from copy import deepcopy


class Interface(metaclass=abc.ABCMeta):
    def __init__(self, config):
        self.config = None
        self.ssh_config = None
        self.ssh = None
        self.session = None
        self.closed = True
        self.init_config(config)

    def init_config(self, config):
        _config = deepcopy(config)
        if _config and 'sshtunnel' in _config:
            _ssh_config = _config.pop('sshtunnel')
            if isinstance(_ssh_config, str):
                config_path, section = _ssh_config.split(':')
                self.ssh_config = get_config(filename=config_path, section=section)
            elif isinstance(_ssh_config, dict):
                self.ssh_config = _ssh_config
            else:
                raise TypeError(type(_ssh_config))
        self.config = _config

    @abc.abstractmethod
    def connect(self, *args, **kwargs):
        """connect to the server"""

    @abc.abstractmethod
    def close(self):
        """close the connection to the server"""

    @staticmethod
    def exclude_keys(d, keys):
        return {k: v for k, v in d.items() if k not in keys}

    def bind(self, host, port):
        if self.ssh_config:
            _ssh_config = self.exclude_keys(self.ssh_config, keys=['ssh_host', 'ssh_port'])
            self.ssh = SSHTunnelForwarder((self.ssh_config['ssh_host'], int(self.ssh_config['ssh_port'])),
                                          remote_bind_address=(host, int(port)),
                                          **_ssh_config)
            self.ssh.start()
            _host = '127.0.0.1'
            _port = self.ssh.local_bind_port
        else:
            _host = host
            _port = port
        return _host, int(_port)

    @staticmethod
    def eval_bool(source):
        assert source in ['True', 'False', True, False], "'source' should be either 'True' or 'False'"
        if type(source) == str:
            return eval(source)
        else:
            return source
