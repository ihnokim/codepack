from codepack.config.config import Config
from codepack.config.alias import Alias
import os
import inspect


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
        cls.init_alias(alias_path=alias_path)
        cls.init_instances()

    @classmethod
    def init_config(cls, config_path: str = None):
        cls.config = cls._get_config(config_path=config_path)

    @staticmethod
    def _get_config(config_path: str = None):
        return Config(config_path=config_path)

    @classmethod
    def get_config(cls, config_path: str = None):
        if config_path:
            _config = cls._get_config(config_path=config_path)
        elif not cls.config:
            cls.init_config()
            _config = cls.config
        else:
            _config = cls.config
        return _config

    @classmethod
    def _get_storage_config(cls, section: str, config_path: str = None):
        _config = cls.get_config(config_path=config_path)
        return _config.get_storage_config(section=section)

    @classmethod
    def init_alias(cls, alias_path: str = None):
        cls.alias = cls._get_alias(alias_path=alias_path)

    @staticmethod
    def _get_alias(alias_path: str = None):
        if alias_path:
            return Alias(data=alias_path)
        else:
            return Alias()

    @classmethod
    def init_instances(cls):
        cls.instances = dict()

    @classmethod
    def set_config(cls, config: Config):
        cls.config = config  # pragma: no cover

    @classmethod
    def set_alias(cls, alias: Alias):
        cls.alias = alias  # pragma: no cover

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
    def get_class_from_alias(cls, alias: str, alias_path: str = None):
        if alias_path:
            _alias = cls._get_alias(alias_path=alias_path)
        elif not cls.alias:
            cls.init_alias()
            _alias = cls.alias
        else:
            _alias = cls.alias
        return _alias[alias]

    @classmethod
    def get_service(cls, section: str, service_type: str, config_path: str = None, alias_path: str = None):
        key = '%s_%s' % (section, service_type)
        if config_path or alias_path or key not in cls.instances:
            storage_config = cls._get_storage_config(section=section, config_path=config_path)
            source = storage_config.pop('source')
            storage_alias = cls.get_alias_from_source(source=source, suffix='storage')
            storage_class = cls.get_class_from_alias(storage_alias, alias_path=alias_path)
            service_class = cls.get_class_from_alias(service_type, alias_path=alias_path)
            item_type = cls.get_class_from_alias(section, alias_path=alias_path)
            storage_instance = storage_class(item_type=item_type, **storage_config)
            service_instance = service_class(storage=storage_instance)
            if config_path is None and alias_path is None:
                cls.instances[key] = service_instance
            return service_instance
        else:
            return cls.instances[key]

    @classmethod
    def get_scheduler(cls, section: str = 'scheduler', config_path: str = None, alias_path: str = None):
        key = section
        if config_path or alias_path or key not in cls.instances:
            storage_config = cls._get_storage_config(section=section, config_path=config_path)
            source = storage_config.pop('source')
            alias = cls.get_alias_from_source(source=source, suffix=section)
            c = cls.get_class_from_alias(alias, alias_path=alias_path)
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
            c = cls.get_class_from_alias(section, alias_path=alias_path)
            employee_config = dict()
            for k, v in storage_config.items():
                if k in inspect.getfullargspec(c.__init__).args:
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
            c = cls.get_class_from_alias(section, alias_path=alias_path)
            if 'path' not in storage_config:
                default_dir = os.path.dirname(os.path.abspath(inspect.getfile(c)))
                storage_config['path'] = os.path.join(default_dir, 'scripts')
            _instance = c(**storage_config)
            if config_path is None and alias_path is None:
                cls.instances[key] = _instance
            return _instance
        else:
            return cls.instances[key]

    @classmethod
    def get_interpreter_manager(cls, section: str = 'interpreter_manager', config_path: str = None, alias_path: str = None):
        key = section
        if config_path or alias_path or key not in cls.instances:
            config_instance = cls.get_config(config_path=config_path)
            config = config_instance.get_config(section=section, config_path=config_path, ignore_error=True)
            kwargs = dict()
            kwargs['path'] = config_instance.get_value(section=section, key='path', config=config)
            if config:
                for k in config:
                    if k not in kwargs:
                        kwargs[k] = config_instance.get_value(section=section, key=k, config=config)
            c = cls.get_class_from_alias(section, alias_path=alias_path)
            _instance = c(**kwargs)
            if config_path is None and alias_path is None:
                cls.instances[key] = _instance
            return _instance
        else:
            return cls.instances[key]

    @classmethod
    def get_logger(cls, name: str = None, config_path: str = None):
        key = 'logger.%s' % (name if name else 'root')
        if config_path or key not in cls.instances:
            if config_path:
                _config = cls._get_config(config_path=config_path)
            elif not cls.config:
                cls.init_config()
                _config = cls.config
            else:
                _config = cls.config
            logger_config = _config.get_logger_config()
            _name = logger_config.get('name', None)
            if name is not None:
                _name = name
            logger = _config.get_logger(config_path=logger_config['path'], name=_name)
            if config_path is None:
                cls.instances[key] = logger
            return logger
        else:
            return cls.instances[key]
