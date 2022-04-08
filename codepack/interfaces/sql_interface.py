from codepack.interfaces.interface import Interface
import abc
import numpy as np
from typing import Optional, Any


class SQLInterface(Interface, metaclass=abc.ABCMeta):
    @staticmethod
    def isnan(value: Any) -> bool:
        ret = False
        try:
            ret = np.isnan(value)
        except Exception:
            pass
        finally:
            return ret

    @staticmethod
    def _enclose(token: str, mark: str = "'", apply: bool = True) -> str:
        if apply:
            return mark + token + mark
        else:
            return token

    @staticmethod
    def encode_sql(custom_op: Optional[dict] = None,
                   default_joint: str = 'and', custom_joint: str = 'and', **kwargs: Any) -> str:
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
