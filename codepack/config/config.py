from configparser import ConfigParser
import os
import logging
from logging.config import dictConfig
import json
import inspect


class Config:
    PREFIX = 'CODEPACK'
    LABEL_CONFIG_DIR = '%s_CONFIG_DIR' % PREFIX
    LABEL_CONFIG_PATH = '%s_CONFIG_PATH' % PREFIX
    LABEL_LOGGER_LOG_DIR = '%s_LOGGER_LOG_DIR' % PREFIX

    def __init__(self, config_path: str = None):
        if config_path:
            self.config_path = self.get_config_path(config_path)
        else:
            self.config_path = None

    @staticmethod
    def parse_config(section: str, config_path: str, ignore_error: bool = False):
        cp = ConfigParser()
        cp.read(config_path)
        if ignore_error and not cp.has_section(section):
            return None
        items = cp.items(section)
        return {item[0]: item[1] for item in items}

    @staticmethod
    def get_logger(config_path: str, name: str = None):
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
    def get_log_dir():
        return os.environ.get(Config.LABEL_LOGGER_LOG_DIR, 'logs')

    def get_config(self, section: str, config_path: str = None, ignore_error: bool = False):
        _config_path = config_path
        if not _config_path:
            tmp = os.environ.get(self.LABEL_CONFIG_PATH, None)
            if tmp:
                _config_path = self.get_config_path(tmp)
        if not _config_path:
            _config_path = self.config_path
        if _config_path:
            return self.parse_config(section=section, config_path=self.get_config_path(_config_path))
        else:
            default_config = self.get_default_config(section=section)
            if default_config is None:
                if ignore_error:
                    return None
                else:
                    raise AttributeError("path of configuration file should be provided in either 'config_path' or os.environ['%s']"
                                         % self.LABEL_CONFIG_PATH)
            else:
                return default_config

    @classmethod
    def get_value(cls, section: str, key: str, config: dict = None, ignore_error: bool = False):
        env = cls.os_env(key=section, value=key)
        if env in os.environ:
            ret = os.environ.get(env, None)
        elif config and key in config:
            ret = config[key]
        else:
            default_config = cls.get_default_config(section=section)
            if default_config is None:
                if ignore_error:
                    return None
                else:
                    raise AssertionError("'%s' information should be provided in os.environ['%s']" % (section, env))
            else:
                if key in default_config:
                    ret = default_config[key]
                elif ignore_error:
                    return None
                else:
                    raise AssertionError("'%s' information should be provided in os.environ['%s']" % (section, env))
        if key == 'path' and section in {'conn', 'alias', 'logger'}:
            ret = cls.get_config_path(ret)
        return ret

    def get_storage_config(self, section: str, config_path: str = None):
        config = self.get_config(section=section, config_path=config_path, ignore_error=True)
        ret = dict()
        ret['source'] = self.get_value(section=section, key='source', config=config)
        if ret['source'] == 'memory':
            pass
        elif ret['source'] == 'file':
            ret['path'] = self.get_value(section=section, key='path', config=config)
        elif ret['source'] == 'mongodb':
            ret['db'] = self.get_value(section=section, key='db', config=config)
            ret['collection'] = self.get_value(section=section, key='collection', config=config)
            conn_config_path = self.get_conn_config_path(config_path=config_path)
            ret['mongodb'] = self.parse_config(section='mongodb', config_path=conn_config_path)
        elif ret['source'] == 'kafka':
            ret['topic'] = self.get_value(section=section, key='topic', config=config)
            conn_config_path = self.get_conn_config_path(config_path=config_path)
            ret['kafka'] = self.parse_config(section='kafka', config_path=conn_config_path)
        elif ret['source'] in {'docker', 's3'}:
            conn_config_path = self.get_conn_config_path(config_path=config_path)
            ret[ret['source']] = self.parse_config(section=ret['source'], config_path=conn_config_path)
        else:
            raise NotImplementedError("'%s' is not implemented" % ret['source'])
        if config:
            for k in config:
                if k not in ret:
                    ret[k] = self.get_value(section=section, key=k, config=config)
        else:
            for k in self.get_default_config(section=section):
                if k not in ret:
                    ret[k] = self.get_value(section=section, key=k, config=config)
        for key, value in {k: v for k, v in os.environ.items() if self.os_env(key=section) in k}.items():
            k = key.replace(self.os_env(key=section), '').lower()
            if k not in ret:
                ret[k] = self.get_value(section=section, key=k)
        return ret

    @classmethod
    def os_env(cls, key: str, value: str = None):
        ret = '%s_%s_' % (cls.PREFIX, key.replace('_', '').upper())
        if value:
            ret += value.upper()
        return ret

    @classmethod
    def get_config_path(cls, path: str):
        ret = path
        if not os.path.exists(path):
            if cls.LABEL_CONFIG_DIR in os.environ:
                ret = os.path.join(os.environ.get(cls.LABEL_CONFIG_DIR), ret)
            else:
                raise AssertionError("config directory should be provided in os.environ['%s']" % cls.LABEL_CONFIG_DIR)
        if not os.path.exists(ret):
            raise FileNotFoundError("'%s' does not exist" % ret)
        return ret

    def get_conn_config_path(self, config_path: str = None):
        conn_config = self.get_config(section='conn', config_path=config_path, ignore_error=True)
        return self.get_value(section='conn', key='path', config=conn_config)

    def get_logger_config(self, config_path: str = None):
        logger_config = self.get_config(section='logger', config_path=config_path, ignore_error=True)
        if not logger_config:
            logger_config = dict()
        logger_config['path'] = self.get_value(section='logger', key='path', config=logger_config, ignore_error=True)
        if logger_config['path'] is None:
            logger_config['path'] = os.path.join(self.get_default_config_dir(), 'logging.json')
        return logger_config

    @classmethod
    def get_default_config_dir(cls):
        return os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(cls))), 'default')

    @classmethod
    def get_default_config(cls, section: str):
        default_config_path = os.path.join(cls.get_default_config_dir(), 'default.ini')
        if os.path.isfile(default_config_path):
            try:
                return cls.parse_config(section=section, config_path=default_config_path)
            except Exception:
                return None
        else:
            return None
