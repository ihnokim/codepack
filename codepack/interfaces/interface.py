from codepack.utils.config.config import Config
import abc
import sshtunnel
from copy import deepcopy
from typing import Any, Union


class Interface(metaclass=abc.ABCMeta):
    def __init__(self, config: dict) -> None:
        self.config = None
        self.ssh_config = None
        self.ssh = None
        self.session = None
        self._closed = True
        self.init_config(config)

    def init_config(self, config: dict) -> None:
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
                raise TypeError(type(_ssh_config))  # pragma: no cover
        self.config = _config

    @abc.abstractmethod
    def connect(self, *args: Any, **kwargs: Any) -> Any:
        """connect to the server"""

    @abc.abstractmethod
    def close(self) -> None:
        """close the connection to the server"""

    def closed(self) -> bool:
        return self._closed

    @staticmethod
    def exclude_keys(d: dict, keys: Union[list, set, dict]) -> dict:
        return {k: v for k, v in d.items() if k not in keys}

    def bind(self, host: str, port: Union[str, int]) -> tuple:
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
    def eval_bool(source: Union[str, bool]) -> bool:
        assert source in ['True', 'False', True, False], "'source' should be either 'True' or 'False'"
        if type(source) == str:
            return eval(source)
        else:
            return source
