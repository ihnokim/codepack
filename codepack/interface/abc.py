import abc
import numpy as np
from sshtunnel import SSHTunnelForwarder


class Interface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self):
        """initialize an instance"""
        self.config = None
        self.ssh_config = None
        self.ssh = None
        self.closed = True

    @abc.abstractmethod
    def connect(self, config, ssh_config=None, **kwargs):
        """connect to the server"""

    @abc.abstractmethod
    def close(self):
        """close the connection to the server"""

    def __del__(self):
        self.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @staticmethod
    def exclude_keys(d, keys):
        return {k: v for k, v in d.items() if k not in keys}

    def set_sshtunnel(self, host, port, ssh_config):
        self.ssh_config = ssh_config
        if self.ssh_config:
            _ssh_config = self.exclude_keys(ssh_config, keys=['ssh_host', 'ssh_port'])
            self.ssh = SSHTunnelForwarder((self.ssh_config['ssh_host'], int(self.ssh_config['ssh_port'])),
                                          remote_bind_address=(host, int(port)),
                                          **_ssh_config)
            self.ssh.start()
            _host = '127.0.0.1'
            _port = self.ssh.local_bind_port
        else:
            _host = host
            _port = port
        return _host, int(_port)

    @staticmethod
    def eval_bool(source):
        assert source in ['True', 'False', True, False], "'source' should be either 'True' or 'False'"
        if type(source) == str:
            return eval(source)
        else:
            return source


class SQLInterface(Interface, metaclass=abc.ABCMeta):
    @staticmethod
    def isnan(value):
        ret = False
        try:
            ret = np.isnan(value)
        except Exception:
            pass
        finally:
            return ret

    @staticmethod
    def _enclose(token, mark="'", apply=True):
        if apply:
            return mark + token + mark
        else:
            return token

    @staticmethod
    def encode_sql(custom_op=None, default_joint='and', custom_joint='and', **kwargs):
        tokens = list()
        for k, v in kwargs.items():
            if type(v) is list:
                quote = True if type(v[0]) == str else False
                template = k + ' in (%s)' % SQLInterface._enclose('%s', mark="'", apply=quote)
                tokens.append(template % SQLInterface._enclose(', ', mark="'", apply=quote).join([str(x) for x in v]))
            else:
                quote = True if type(v) == str else False
                template = k + ' = %s' % SQLInterface._enclose('%s', mark="'", apply=quote)
                tokens.append(template % v)

        if custom_op:
            for k, v in custom_op.items():
                custom_tokens = list()
                for op, value in v.items():
                    quote = True if type(value) == str else False
                    template = '%s %s ' + SQLInterface._enclose('%s', mark="'", apply=quote)
                    custom_tokens.append(template % (k, op, value))
                custom_exp = (' %s ' % custom_joint.strip()).join(custom_tokens)
                if len(custom_tokens) >= 2:
                    custom_exp = '(' + custom_exp + ')'
                tokens.append(custom_exp)
        return (' %s ' % default_joint.strip()).join(tokens)
