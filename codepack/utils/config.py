from configparser import ConfigParser
from codepack.interface import MongoDB
import os


def get_config(filename, section):
    cp = ConfigParser()
    cp.read(filename)
    items = cp.items(section)
    return {item[0]: item[1] for item in items}


def get_config_assertion_error_message(x):
    return "'%s' should be given as an argument or defined in a configuration file in 'config_path' " \
           "or os.environ['CODEPACK_CONFIG_PATH']" % x


def get_default_config(section, config_path=None):
    if not config_path:
        config_path = os.environ.get('CODEPACK_CONFIG_PATH', None)
    if config_path:
        config = get_config(config_path, section=section)
        return config
    else:
        return None


def get_default_value(section, key, config=None):
    env = 'CODEPACK_%s_%s' % (section.upper(), key.upper())
    if config:
        ret = config[key]
    elif env in os.environ:
        ret = os.environ.get(env, None)
    else:
        raise AssertionError("'%s' information should be provided in a configuration file in 'config_path' "
                             "or os.environ['%s']" % (section, env))
    return ret


def get_default_service_config(section, config_path=None):
    config = get_default_config(section=section, config_path=config_path)
    ret = dict()
    ret['source'] = get_default_value(section=section, key='source', config=config).upper()
    if ret['source'] == 'MEMORY':
        pass
    elif ret['source'] == 'FILE':
        ret['path'] = get_default_value(section=section, key='path', config=config)
    elif ret['source'] == 'MONGODB':
        ret['db'] = get_default_value(section=section, key='db', config=config)
        ret['collection'] = get_default_value(section=section, key='collection', config=config)
        conn_config = get_default_config(section='conn', config_path=config_path)
        conn_config_path = get_default_value(section='conn', key='path', config=conn_config)
        mongodb_config = get_config(conn_config_path, 'mongodb')
        ret['mongodb'] = MongoDB(config=mongodb_config)
    else:
        raise NotImplementedError("'%s' is not implemented" % ret['source'])
    return ret
