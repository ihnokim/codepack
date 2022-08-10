from codepack.utils.config.config import Config
import abc
from copy import deepcopy
from typing import Any, Union, TypeVar


SSHTunnelForwarder = TypeVar('SSHTunnelForwarder', bound='sshtunnel.SSHTunnelForwarder')


class Interface(metaclass=abc.ABCMeta):
    def __init__(self, config: dict) -> None:
        self.config = None
        self.ssh_config = None
        self.session = None
        self.ssh = None
        self._closed = True
        self.set_config(config)

    def set_config(self, config: dict) -> None:
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
        self.config = deepcopy(config)

    @staticmethod
    def create_sshtunnel(host: str, port: int, ssh_host: str, ssh_port: int, *args, **kwargs) -> SSHTunnelForwarder:
        import sshtunnel
        return sshtunnel.SSHTunnelForwarder(remote_bind_address=(host, port),
                                            ssh_address_or_host=(ssh_host, ssh_port),
                                            *args, **kwargs)

    @staticmethod
    def inspect_config_for_sshtunnel(config: dict, host_key: str = 'host', port_key: str = 'port') -> None:
        for key in [host_key, port_key]:
            assert key in config.keys(), "'%s' should be included in config to set sshtunnel" % key

    def set_sshtunnel(self, host: str, port: int) -> tuple:
        _ssh_config = {k: v for k, v in self.ssh_config.items()}
        if 'ssh_port' in _ssh_config:
            _ssh_config['ssh_port'] = int(_ssh_config['ssh_port'])
        self.ssh = self.create_sshtunnel(host=host, port=port, **_ssh_config)
        self.ssh.start()
        return '127.0.0.1', self.ssh.local_bind_port

    @abc.abstractmethod
    def connect(self, *args: Any, **kwargs: Any) -> Any:
        """initialize session"""

    @abc.abstractmethod
    def close(self) -> None:
        """close session"""

    def close_sshtunnel(self):
        if self.ssh:
            self.ssh.close()

    def closed(self) -> bool:
        return self._closed

    @staticmethod
    def exclude_keys(d: dict, keys: Union[list, set, dict]) -> dict:
        return {k: v for k, v in d.items() if k not in keys}

    @staticmethod
    def eval_bool(source: Union[str, bool]) -> bool:
        assert source in ['True', 'False', True, False], "'source' should be either 'True' or 'False'"
        if type(source) == str:
            return eval(source)
        else:
            return source
