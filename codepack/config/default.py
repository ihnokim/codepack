from codepack.config.config import Config
from codepack.config.alias import Alias
import os
from inspect import getfullargspec


class Default:
    config = None
    alias = None
    instances = dict()

    @classmethod
    def __init__(cls, config_path: str = None, alias_path: str = None):
        cls.init(config_path=config_path, alias_path=alias_path)

    @classmethod
    def init(cls, config_path: str = None, alias_path: str = None):
        cls.init_config(config_path=config_path)
        cls.init_alias(config_path=config_path, alias_path=alias_path)
        cls.init_instances()

    @classmethod
    def init_config(cls, config_path: str = None):
        cls.config = cls._get_config(config_path=config_path)

    @staticmethod
    def _get_config(config_path: str = None):
        return Config(config_path=config_path)

    @classmethod
    def _get_storage_config(cls, section: str, config_path: str = None):
        if config_path:
            _config = cls._get_config(config_path=config_path)
        elif not cls.config:
            cls.init_config()
            _config = cls.config
        else:
            _config = cls.config
        return _config.get_storage_config(section=section)

    @classmethod
    def init_alias(cls, config_path: str = None, alias_path: str = None):
        cls.alias = cls._get_alias(config_path=config_path, alias_path=alias_path)

    @staticmethod
    def _get_alias(config_path: str = None, alias_path: str = None):
        if alias_path:
            return Alias(data=alias_path)
        elif config_path:
            return Alias(data=config_path)
        else:
            return Alias()

    @classmethod
    def init_instances(cls):
        cls.instances = dict()

    @classmethod
    def set_config(cls, config: Config):
        cls.config = config

    @classmethod
    def set_alias(cls, alias: Alias):
        cls.alias = alias

    @staticmethod
    def get_alias_from_source(source: str, prefix: str = None, suffix: str = None):
        if source == 'mongodb':
            ret = 'mongo'
        else:
            ret = source
        if prefix:
            ret = '%s_%s' % (prefix, ret)
        if suffix:
            ret = '%s_%s' % (ret, suffix)
        return ret

    @classmethod
    def get_class_from_alias(cls, alias: str, config_path: str = None, alias_path: str = None):
        if config_path or alias_path:
            _alias = cls._get_alias(config_path=config_path, alias_path=alias_path)
        elif not cls.alias:
            cls.init_alias()
            _alias = cls.alias
        else:
            _alias = cls.alias
        return _alias[alias]

    @classmethod
    def get_storage_instance(cls, section: str, instance_type: str, config_path: str = None, alias_path: str = None):
        key = '%s_%s' % (section, instance_type)
        if config_path or alias_path or key not in cls.instances:
            storage_config = cls._get_storage_config(section=section, config_path=config_path)
            source = storage_config.pop('source')
            alias = cls.get_alias_from_source(source=source, suffix=instance_type)
            c = cls.get_class_from_alias(alias, config_path=config_path, alias_path=alias_path)
            item_type = cls.get_class_from_alias(section, config_path=config_path, alias_path=alias_path)
            _instance = c(item_type=item_type, **storage_config)
            if config_path is None and alias_path is None:
                cls.instances[key] = _instance
            return _instance
        else:
            return cls.instances[key]

    @classmethod
    def get_scheduler(cls, section: str = 'scheduler', config_path: str = None, alias_path: str = None):
        key = section
        if config_path or alias_path or key not in cls.instances:
            storage_config = cls._get_storage_config(section=section, config_path=config_path)
            source = storage_config.pop('source')
            alias = cls.get_alias_from_source(source=source, suffix=section)
            c = cls.get_class_from_alias(alias, config_path=config_path, alias_path=alias_path)
            if 'CODEPACK_SCHEDULER_SUPERVISOR' in os.environ:
                storage_config['supervisor'] = os.environ['CODEPACK_SCHEDULER_SUPERVISOR']
            _instance = c(**storage_config)
            if config_path is None and alias_path is None:
                cls.instances[key] = _instance
            return _instance
        else:
            return cls.instances[key]

    @classmethod
    def get_employee(cls, section: str, config_path: str = None, alias_path: str = None):
        key = section
        if 'worker' in section:
            kafka_type = 'consumer'
        elif 'supervisor' in section:
            kafka_type = 'producer'
        else:
            raise NotImplementedError("'%s' is unknown")
        if config_path or alias_path or key not in cls.instances:
            storage_config = cls._get_storage_config(section=section, config_path=config_path)
            storage_config.pop('source')
            kafka_config = storage_config.pop('kafka')
            c = cls.get_class_from_alias(section, config_path=config_path, alias_path=alias_path)
            employee_config = dict()
            for k, v in storage_config.items():
                if k in getfullargspec(c.__init__).args:
                    employee_config[k] = v
                else:
                    kafka_config[k] = v
            employee_config[kafka_type] = kafka_config
            _instance = c(**employee_config)
            if config_path is None and alias_path is None:
                cls.instances[key] = _instance
            return _instance
        else:
            return cls.instances[key]

    @classmethod
    def get_docker_manager(cls, section: str = 'docker_manager', config_path: str = None, alias_path: str = None):
        key = section
        if config_path or alias_path or key not in cls.instances:
            storage_config = cls._get_storage_config(section=section, config_path=config_path)
            storage_config.pop('source')
            c = cls.get_class_from_alias(section, config_path=config_path, alias_path=alias_path)
            _instance = c(**storage_config)
            if config_path is None and alias_path is None:
                cls.instances[key] = _instance
            return _instance
        else:
            return cls.instances[key]