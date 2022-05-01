from codepack.utils.config.config import Config
from codepack.utils.config.alias import Alias
import inspect
from docker.errors import DockerException
from typing import Optional, TypeVar


Service = TypeVar('Service', bound='codepack.plugins.service.Service')
Scheduler = TypeVar('Scheduler', bound='codepack.plugins.scheduler.Scheduler')
Employee = TypeVar('Employee', bound='codepack.plugins.employee.Employee')
Manager = TypeVar('Manager', bound='codepack.plugins.manager.Manager')
Logger = TypeVar('Logger', bound='logging.Logger')


class Default:
    config = None
    alias = None
    instances = dict()

    @classmethod
    def __init__(cls, config_path: Optional[str] = None, alias_path: Optional[str] = None) -> None:
        cls.init(config_path=config_path, alias_path=alias_path)

    @classmethod
    def init(cls, config_path: Optional[str] = None, alias_path: Optional[str] = None) -> None:
        cls.init_config(config_path=config_path)
        cls.init_alias(alias_path=alias_path)
        cls.init_instances()

    @classmethod
    def init_config(cls, config_path: Optional[str] = None) -> None:
        cls.config = Config(config_path=config_path)

    @classmethod
    def get_config_instance(cls, config_path: Optional[str] = None) -> Config:
        if config_path:
            _config = Config(config_path=config_path)
        elif not cls.config:
            cls.init_config()
            _config = cls.config
        else:
            _config = cls.config
        return _config

    @classmethod
    def init_alias(cls, alias_path: Optional[str] = None) -> None:
        cls.alias = Alias(data=alias_path)

    @classmethod
    def get_alias_instance(cls, alias_path: Optional[str] = None) -> Alias:
        if alias_path:
            _alias = Alias(data=alias_path)
        elif not cls.alias:
            cls.init_alias()
            _alias = cls.alias
        else:
            _alias = cls.alias
        return _alias

    @classmethod
    def init_instances(cls) -> None:
        cls.instances = dict()

    @classmethod
    def set_config(cls, config: Config) -> None:
        cls.config = config  # pragma: no cover

    @classmethod
    def set_alias(cls, alias: Alias) -> None:
        cls.alias = alias  # pragma: no cover

    @staticmethod
    def get_alias_from_source(source: str, prefix: Optional[str] = None, suffix: Optional[str] = None) -> str:
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
    def get_class_from_alias(cls, alias: str, alias_path: Optional[str] = None) -> type:
        return cls.get_alias_instance(alias_path=alias_path)[alias]

    @classmethod
    def get_service(cls, section: str, service_type: str,
                    config_path: Optional[str] = None, alias_path: Optional[str] = None) -> Service:
        key = '%s_%s' % (section, service_type)
        if config_path or alias_path or key not in cls.instances:
            config = cls.get_config_instance(config_path=config_path)
            storage_config = config.get_storage_config(section=section, config_path=config_path)
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
    def get_scheduler(cls, section: str = 'scheduler',
                      config_path: Optional[str] = None, alias_path: Optional[str] = None) -> Scheduler:
        key = section
        if config_path or alias_path or key not in cls.instances:
            config = cls.get_config_instance(config_path=config_path)
            storage_config = config.get_storage_config(section=section, config_path=config_path)
            source = storage_config.pop('source')
            storage_alias = cls.get_alias_from_source(source=source, suffix='storage')
            storage_class = cls.get_class_from_alias(storage_alias, alias_path=alias_path)
            scheduler_class = cls.get_class_from_alias(section, alias_path=alias_path)
            scheduler_config = dict()
            remaining_config = dict()
            for k, v in storage_config.items():
                if k in inspect.getfullargspec(scheduler_class.__init__).args:
                    scheduler_config[k] = v
                else:
                    remaining_config[k] = v
            storable_job_class = cls.get_class_from_alias('storable_job')
            storage_instance = storage_class(item_type=storable_job_class, key='id', **remaining_config)
            jobstore_class = cls.get_class_from_alias('jobstore')
            jobstore_instance = jobstore_class(storage=storage_instance)
            _instance = scheduler_class(jobstore=jobstore_instance, **scheduler_config)
            if config_path is None and alias_path is None:
                cls.instances[key] = _instance
            return _instance
        else:
            return cls.instances[key]

    @classmethod
    def get_employee(cls, section: str, config_path: Optional[str] = None,
                     alias_path: Optional[str] = None) -> Employee:
        key = section
        if 'worker' in section:
            messenger_type = 'consumer'
        elif 'supervisor' in section:
            messenger_type = 'producer'
        else:
            raise NotImplementedError("'%s' is unknown")  # pragma: no cover
        if config_path or alias_path or key not in cls.instances:
            config = cls.get_config_instance(config_path=config_path)
            storage_config = config.get_storage_config(section=section, config_path=config_path)
            source = storage_config.pop('source')
            storage_alias = cls.get_alias_from_source(source=source, suffix='messenger')
            storage_class = cls.get_class_from_alias(storage_alias, alias_path=alias_path)
            if source == 'memory':
                conn_config = dict()
            else:
                conn_config = storage_config.pop(source)
            employee_class = cls.get_class_from_alias(section, alias_path=alias_path)
            employee_config = dict()
            for k, v in storage_config.items():
                if k in inspect.getfullargspec(employee_class.__init__).args:
                    employee_config[k] = v
                else:
                    conn_config[k] = v
            if source == 'kafka':
                storage_instance = storage_class(**{messenger_type: conn_config})
            else:
                storage_instance = storage_class(**conn_config)
            _instance = employee_class(messenger=storage_instance, **employee_config)
            if config_path is None and alias_path is None:
                cls.instances[key] = _instance
            return _instance
        else:
            return cls.instances[key]

    @classmethod
    def get_docker_manager(cls, section: str = 'docker_manager',
                           config_path: Optional[str] = None, alias_path: Optional[str] = None) -> Manager:
        key = section
        if config_path or alias_path or key not in cls.instances:
            config = cls.get_config_instance(config_path=config_path)
            storage_config = config.get_storage_config(section=section, config_path=config_path)
            storage_config.pop('source')
            manager_class = cls.get_class_from_alias(section, alias_path=alias_path)
            try:
                _instance = manager_class(**storage_config)
            except DockerException:
                return None
            if config_path is None and alias_path is None:
                cls.instances[key] = _instance
            return _instance
        else:
            return cls.instances[key]

    @classmethod
    def get_interpreter_manager(cls, section: str = 'interpreter_manager',
                                config_path: Optional[str] = None, alias_path: Optional[str] = None) -> Manager:
        key = section
        if config_path or alias_path or key not in cls.instances:
            config = cls.get_config_instance(config_path=config_path)
            manager_config = config.get_config(section=section, config_path=config_path, ignore_error=True)
            manager_class = cls.get_class_from_alias(section, alias_path=alias_path)
            instance = manager_class(**manager_config)
            if config_path is None and alias_path is None:
                cls.instances[key] = instance
            return instance
        else:
            return cls.instances[key]

    @classmethod
    def get_logger(cls, name: Optional[str] = None, config_path: Optional[str] = None) -> Logger:
        key = 'logger.%s' % (name if name else 'root')
        if config_path or key not in cls.instances:
            config = cls.get_config_instance(config_path=config_path)
            logger_config = config.get_config('logger', config_path=config_path)
            logger_name = logger_config.get('name', None)
            if name is not None:
                logger_name = name
            logger = config.get_logger(config_path=logger_config['config_path'], name=logger_name)
            if config_path is None:
                cls.instances[key] = logger
            return logger
        else:
            return cls.instances[key]
