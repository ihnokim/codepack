import abc
import numpy as np


class Interface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self, config, **kwargs):
        """Initialize an instance from config."""

    @abc.abstractmethod
    def connect(self, config, **kwargs):
        """Connect to the server."""

    @abc.abstractmethod
    def close(self):
        """Close the connection to the server."""

    @abc.abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


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
