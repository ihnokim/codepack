from codepack.config.config import Config
import abc
import sshtunnel
from copy import deepcopy


class Interface(metaclass=abc.ABCMeta):
    def __init__(self, config):
        self.config = None
        self.ssh_config = None
        self.ssh = None
        self.session = None
        self._closed = True
        self.init_config(config)

    def init_config(self, config):
        _config = deepcopy(config)
        if _config and 'sshtunnel' in _config:
            _ssh_config = _config.pop('sshtunnel')
            if isinstance(_ssh_config, str):
                _config_path, section = _ssh_config.split(':')
                config_path = Config.get_config_path(path=_config_path)
                self.ssh_config = Config.parse_config(section=section, config_path=config_path)
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

    def closed(self):
        return self._closed

    @staticmethod
    def exclude_keys(d, keys):
        return {k: v for k, v in d.items() if k not in keys}

    def bind(self, host, port):
        if self.ssh_config:
            _ssh_config = self.exclude_keys(self.ssh_config, keys=['ssh_host', 'ssh_port'])
            self.ssh = sshtunnel.SSHTunnelForwarder((self.ssh_config['ssh_host'], int(self.ssh_config['ssh_port'])),
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
