from configparser import ConfigParser
import os
# import logging
# from logging.config import dictConfig
from typing import Optional, Dict, Tuple, List
import re
from copy import deepcopy


class Config:
    PREFIX = 'CODEPACK'
    DELIMITER = '__'
    LABEL_CONFIG_PATH = f'{PREFIX}_CONFIG_PATH'
    LABEL_LOGGER_LOG_DIR = DELIMITER.join([PREFIX, 'LOGGER', 'LOG_DIR'])
    ACCEPTABLE_CHARACTERS = set('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_')
    REPLACEMENT_CHARACTER = '_'

    def __init__(self, config_path: Optional[str] = None) -> None:
        self.config_path: Optional[str]
        self.snapshot: Dict[str, Dict[str, str]] = dict()
        self.set_config_path(path=config_path)
        self.refresh()

    def get_config_path(self) -> Optional[str]:
        return self.config_path

    def set_config_path(self, path: Optional[str] = None) -> None:
        if path:
            if not os.path.exists(path):
                raise FileNotFoundError("'%s' does not exist" % path)
            else:
                self.config_path = path
        elif self.LABEL_CONFIG_PATH in os.environ:
            self.set_config_path(path=os.environ.get(self.LABEL_CONFIG_PATH))
        else:
            self.config_path = None

    @staticmethod
    def parse_config(path: str) -> Dict[str, Dict[str, str]]:
        config = dict()
        cp = ConfigParser()
        cp.read(path)
        for section in cp.sections():
            config[section] = {item[0]: item[1] for item in cp.items(section)}
        return config

    @classmethod
    def os_env(cls, section: str, key: Optional[str] = None) -> str:
        ret = ['', '']
        tokens = [section]
        if key:
            tokens.append(key)
        for i, token in enumerate(tokens):
            _token = token.strip().upper()
            invalid_characters = list()
            for c in token:
                if c not in cls.ACCEPTABLE_CHARACTERS:
                    invalid_characters.append(c)
            for ic in invalid_characters:
                _token = _token.replace(ic, cls.REPLACEMENT_CHARACTER)
            cls._assert_valid_os_env_token(token=_token)
            ret[i] = _token
        return cls.DELIMITER.join([cls.PREFIX, ret[0], ret[1]])

    @classmethod
    def verify_os_env(cls, os_env: str) -> bool:
        tokens = os_env.split(cls.DELIMITER)
        try:
            if len(tokens) != 3:
                raise ValueError(f"'{os_env}' is supposed to have two '{cls.DELIMITER}'s")
            cls._assert_valid_os_env_token(tokens[1])
            if tokens[2]:
                cls._assert_valid_os_env_token(tokens[2])
            return True
        except Exception as e:
            print(e)
        return False

    @classmethod
    def _assert_valid_os_env_token(cls, token: str) -> None:
        pat = re.compile(f'.*{cls.DELIMITER}+.*')
        if pat.match(token):
            raise ValueError(f"Consecutive {cls.REPLACEMENT_CHARACTER} are not allowed")
        if cls.REPLACEMENT_CHARACTER in {token[0], token[-1]}:
            raise ValueError(f"Using {cls.REPLACEMENT_CHARACTER} at both ends is not allowed")
        pat = re.compile('^[A-Z]+[A-Z0-9_]*$')
        if not pat.match(token):
            raise ValueError(f"'{token}' is invalid")

    @classmethod
    def collect_valid_os_envs(cls) -> List[str]:
        ret = list()
        pat = re.compile(f'^{cls.PREFIX}{cls.DELIMITER}[A-Z0-9_]+{cls.DELIMITER}[A-Z0-9_]*$')
        for os_env in [x for x in os.environ.keys() if f'{cls.PREFIX}{cls.DELIMITER}' in x]:
            if pat.match(os_env) and cls.verify_os_env(os_env):
                ret.append(os_env)
        return ret

    @classmethod
    def parse_os_env(cls, os_env) -> Tuple[str, Optional[str], Optional[str]]:
        _, section, key = os_env.split(cls.DELIMITER)
        if key:
            value = os.environ[os_env]
            return section.lower(), key.lower(), value
        else:
            return section.lower(), None, None

    def refresh(self) -> None:
        if self.config_path:
            config = self.parse_config(path=self.config_path)
        else:
            config = dict()
        os_envs = self.collect_valid_os_envs()
        for os_env in os_envs:
            section, key, value = self.parse_os_env(os_env=os_env)
            if section not in config:
                config[section] = dict()
            if key:
                config[section][key] = value
        self.snapshot = config

    def get_config(self, section: Optional[str] = None) -> Optional[Dict[str, str]]:
        if section is None:
            return None
        elif section not in self.snapshot:
            raise KeyError(f"Section '{section}' not found in configuration")
        else:
            return deepcopy(self.snapshot[section])

    def list_sections(self) -> List[str]:
        return list(self.snapshot.keys())

    def list_keys(self, section: str) -> List[str]:
        return list(self.snapshot[section].keys())
