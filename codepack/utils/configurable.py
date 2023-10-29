import abc
from typing import Dict, Any, Optional
from codepack.utils.config import Config


class Configurable(metaclass=abc.ABCMeta):
    @classmethod
    @abc.abstractmethod
    def parse_config(cls, config: Dict[str, str]) -> Dict[str, Any]:
        pass  # pragma: no cover

    @classmethod
    def get_instance(cls, config: Optional[Dict[str, str]] = None) -> 'Configurable':
        _config = cls.parse_config(config=config) if config else None
        if _config:
            return cls(**_config)
        else:
            return cls()

    @classmethod
    def from_config(cls, section: Optional[str] = None) -> 'Configurable':
        config = Config()
        _config = config.get_config(section=section)
        return cls.get_instance(config=_config)
