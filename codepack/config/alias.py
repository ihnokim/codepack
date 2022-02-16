from importlib import import_module
from codepack.config import Config
import os


class Alias:
    PREFIX = 'ALIAS'

    def __init__(self, data=None):
        self.aliases = None
        if isinstance(data, str):
            _aliases = Config.parse_config(section='alias', config_path=data)
            if len(_aliases) == 1 and 'path' in _aliases:
                alias_path = Config.get_value(section='alias', key='path', config=_aliases)
                aliases = Config.parse_config(section='alias', config_path=alias_path)
            else:
                aliases = _aliases
            self.aliases = aliases
        elif isinstance(data, dict):
            self.aliases = data
        elif data is None:
            pass
        else:
            raise TypeError(type(data))  # pragma: no cover

    @classmethod
    def get_env(cls, item):
        return ('%s_%s_%s' % (Config.PREFIX, cls.PREFIX, item)).upper()

    @staticmethod
    def get_class(module, name):
        return getattr(import_module(module), name)

    def __getitem__(self, item):
        if self.aliases:
            path = self.aliases[item]
        elif self.get_env(item) in os.environ:
            path = os.environ.get(self.get_env(item))
        elif '%s_ALIAS_PATH' % Config.PREFIX in os.environ:
            aliases = Config.parse_config(section='alias', config_path=os.environ['%s_ALIAS_PATH' % Config.PREFIX])
            path = aliases[item]
        elif Config.LABEL_CONFIG_PATH in os.environ:
            config = Config.parse_config(section='alias',
                                         config_path=Config.get_config_path(os.environ.get(Config.LABEL_CONFIG_PATH)),
                                         ignore_error=True)
            alias_path = Config.get_value(section='alias', key='path', config=config)
            aliases = Config.parse_config(section='alias', config_path=alias_path)
            path = aliases[item]
        else:
            raise AttributeError("%s not found in os.environ['%s'], os.environ['%s'], and os.environ['%s']"
                                 % (item, self.get_env(item), '%s_ALIAS_PATH' % Config.PREFIX, Config.LABEL_CONFIG_PATH))
        tokens = path.split('.')
        module = '.'.join(tokens[:-1])
        name = tokens[-1]
        return self.get_class(module, name)
