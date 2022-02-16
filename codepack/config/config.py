from configparser import ConfigParser
import os


class Config:
    PREFIX = 'CODEPACK'
    LABEL_CONFIG_DIR = '%s_CONFIG_DIR' % PREFIX
    LABEL_CONFIG_PATH = '%s_CONFIG_PATH' % PREFIX

    def __init__(self, config_path: str = None):
        self.config_path = config_path

    @staticmethod
    def parse_config(section: str, config_path: str, ignore_error: bool = False):
        cp = ConfigParser()
        cp.read(config_path)
        if ignore_error and not cp.has_section(section):
            return None
        items = cp.items(section)
        return {item[0]: item[1] for item in items}

    def get_config(self, section: str, config_path: str = None, ignore_error: bool = False):
        _config_path = config_path
        if not _config_path:
            _config_path = self.config_path
        if not _config_path:
            tmp = os.environ.get(self.LABEL_CONFIG_PATH, None)
            if tmp:
                _config_path = self.get_config_path(tmp)
        if _config_path:
            return self.parse_config(section=section, config_path=_config_path)
        elif ignore_error:
            return None
        else:
            raise AttributeError("path of configuration file should be provided in either 'config_path' or os.environ['%s']"
                                 % self.LABEL_CONFIG_PATH)

    @classmethod
    def get_value(cls, section: str, key: str, config: dict = None):
        env = '%s_%s_%s' % (cls.PREFIX, section.upper(), key.upper())
        if config:
            ret = config[key]
        elif env in os.environ:
            ret = os.environ.get(env, None)
        else:
            raise AssertionError("'%s' information should be provided in os.environ['%s']" % (section, env))
        if key == 'path':
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
            conn_config = self.get_config(section='conn', config_path=config_path, ignore_error=True)
            conn_config_path = self.get_value(section='conn', key='path', config=conn_config)
            ret['mongodb'] = self.parse_config(section='mongodb', config_path=conn_config_path)
        elif ret['source'] == 'kafka':
            ret['topic'] = self.get_value(section=section, key='topic', config=config)
            conn_config = self.get_config(section='conn', config_path=config_path, ignore_error=True)
            conn_config_path = self.get_value(section='conn', key='path', config=conn_config)
            ret['kafka'] = self.parse_config(section='kafka', config_path=conn_config_path)
        else:
            raise NotImplementedError("'%s' is not implemented" % ret['source'])
        if config:
            for k in config:
                if k not in ret:
                    ret[k] = self.get_value(section=section, key=k, config=config)
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
