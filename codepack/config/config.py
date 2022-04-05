from configparser import ConfigParser
import os
import logging
from logging.config import dictConfig
import json
import inspect
from typing import Optional


class Config:
    PREFIX = 'CODEPACK'
    LABEL_CONFIG_DIR = '%s_CONFIG_DIR' % PREFIX
    LABEL_CONFIG_PATH = '%s_CONFIG_PATH' % PREFIX
    LABEL_LOGGER_LOG_DIR = '%s_LOGGER_LOG_DIR' % PREFIX

    def __init__(self, config_path: Optional[str] = None) -> None:
        if config_path:
            self.config_path = self.get_config_path(path=config_path)
        else:
            self.config_path = None

    @staticmethod
    def parse_config(section: str, config_path: str, ignore_error: bool = False) -> Optional[dict]:
        cp = ConfigParser()
        cp.read(config_path)
        if ignore_error and not cp.has_section(section):
            return None
        items = cp.items(section)
        return {item[0]: item[1] for item in items}

    def get_config(self, section: str, config_path: Optional[str] = None,
                   ignore_error: Optional[bool] = False) -> Optional[dict]:
        overwrite_with_os_env = False
        parse_default_config = False
        _config_path = None
        if config_path:
            _config_path = self.get_config_path(path=config_path)
        elif self.config_path:
            _config_path = self.config_path
        elif self.LABEL_CONFIG_PATH in os.environ:
            overwrite_with_os_env = True
            _config_path = self.get_config_path(os.environ.get(self.LABEL_CONFIG_PATH))
        else:
            overwrite_with_os_env = True
            parse_default_config = True
        if _config_path:
            ret = self.parse_config(section=section, config_path=self.get_config_path(_config_path))
        elif parse_default_config:
            ret = self.get_default_config(section=section)
        elif ignore_error:
            return None
        else:
            raise AttributeError(
                "path of configuration file should be provided in either 'config_path' or os.environ['%s']"
                % self.LABEL_CONFIG_PATH)
        if ret and overwrite_with_os_env:
            ret = self.collect_values(section=section, config=ret, ignore_error=ignore_error)
        return ret

    @classmethod
    def _os_env_missing_error_message(cls, section: str, key: str) -> str:
        return "'%s' information should be provided in os.environ['%s']" % (section, cls.os_env(key=section, value=key))

    @classmethod
    def _assert_mandatory_keys(cls, section: str, mandatory_keys: list, config: dict) -> None:
        for key in mandatory_keys:
            assert key in config, cls._os_env_missing_error_message(section=section, key=key)

    @classmethod
    def collect_value(cls, section: str, key: str, config: dict, ignore_error: bool = False) -> Optional[str]:
        env = cls.os_env(key=section, value=key)
        if env in os.environ:
            value = os.environ.get(env)
        elif key in config:
            value = config[key]
        else:
            default_config = cls.get_default_config(section=section)
            if default_config and key in default_config:
                value = default_config[key]
            elif ignore_error:
                return None
            else:
                raise AssertionError(cls._os_env_missing_error_message(section=section, key=key))
        if section in {'conn', 'alias', 'logger'} and key in {'path', 'config_path'}:
            value = cls.get_config_path(value)
        return value

    @classmethod
    def collect_values(cls, section: str, config: dict, ignore_error: bool = False) -> dict:
        values = dict()
        for key in config.keys():
            values[key] = cls.collect_value(section=section, key=key, config=config, ignore_error=ignore_error)
        for key, value in {k: v for k, v in os.environ.items() if cls.os_env(key=section) in k}.items():
            k = key.replace(cls.os_env(key=section), '').lower()
            if k not in values:
                values[k] = cls.collect_value(section=section, key=k, config=dict())
        default_config = cls.get_default_config(section=section)
        for key, value in default_config.items():
            if key not in values:
                values[key] = value
        return values

    def get_storage_config(self, section: str, config_path: Optional[str] = None) -> dict:
        config = self.get_config(section=section, config_path=config_path)
        source = config.get('source', None)
        if source == 'memory':
            pass
        elif source == 'file':
            self._assert_mandatory_keys(section=section, mandatory_keys=['path'], config=config)
        elif source == 'mongodb':
            self._assert_mandatory_keys(section=section, mandatory_keys=['db', 'collection'], config=config)
            config[source] = self.get_config(section=source, config_path=config_path)
        elif source == 'kafka':
            self._assert_mandatory_keys(section=section, mandatory_keys=['topic'], config=config)
            config[source] = self.get_config(section=source, config_path=config_path)
        elif source in {'docker', 's3'}:
            config[source] = self.get_config(section=source, config_path=config_path)
        else:
            raise NotImplementedError("'%s' source is not implemented" % source)
        return config

    @staticmethod
    def get_logger(config_path: str, name: Optional[str] = None) -> logging.Logger:
        with open(config_path, 'r') as f:
            config = json.load(f)
            if 'handlers' in config:
                for handler in config['handlers'].values():
                    for k, v in handler.items():
                        if k == 'filename':
                            log_dir = Config.get_log_dir()
                            log_file = os.path.join(log_dir, v)
                            if not os.path.exists(log_dir):
                                os.makedirs(log_dir)
                            handler.update(filename=log_file)
            dictConfig(config)
        return logging.getLogger(name=name)

    @staticmethod
    def get_log_dir() -> str:
        return os.environ.get(Config.LABEL_LOGGER_LOG_DIR, 'logs')

    @classmethod
    def get_default_config_dir(cls) -> str:
        return os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(cls))), 'default')

    @classmethod
    def get_default_config_path(cls) -> str:
        return os.path.join(cls.get_default_config_dir(), 'default.ini')

    @classmethod
    def get_default_config(cls, section: str) -> Optional[dict]:
        default_config_path = cls.get_default_config_path()
        ret = None
        try:
            if os.path.isfile(default_config_path):
                ret = cls.parse_config(section=section, config_path=default_config_path)
                default_dir = cls.get_default_config_dir()
                if section == 'callback' and 'source' in ret and ret['source'] == 'file' and 'path' not in ret:
                    ret['path'] = os.path.join(default_dir, 'scripts')
                elif section == 'docker_manager' and 'path' not in ret:
                    ret['path'] = os.path.join(default_dir, 'scripts')
                elif section == 'logger' and 'config_path' not in ret:
                    ret['config_path'] = os.path.join(default_dir, 'logging.json')
                elif section == 'worker' and 'script_path' not in ret:
                    ret['script_path'] = os.path.join(default_dir, 'scripts/run_snapshot.py')
        except Exception:
            return None
        finally:
            return ret

    @classmethod
    def os_env(cls, key: str, value: Optional[str] = None) -> str:
        ret = '%s_%s_' % (cls.PREFIX, key.replace('_', '').upper())
        if value:
            ret += value.upper()
        return ret

    @classmethod
    def get_config_path(cls, path: str) -> str:
        ret = path
        if not os.path.exists(path):
            if cls.LABEL_CONFIG_DIR in os.environ:
                ret = os.path.join(os.environ.get(cls.LABEL_CONFIG_DIR), ret)
            else:
                raise AssertionError("config directory should be provided in os.environ['%s']" % cls.LABEL_CONFIG_DIR)
        if not os.path.exists(ret):
            raise FileNotFoundError("'%s' does not exist" % ret)
        return ret
