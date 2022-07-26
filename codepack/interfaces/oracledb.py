from codepack.interfaces.sql_interface import SQLInterface
import cx_Oracle
from functools import partial
from typing import Any, Union, Optional


def make_named_row(names: list, *args: Any) -> dict:
    if len(names) != len(args):
        raise Exception('len(names) != len(args)')
    return dict(zip(names, args))


class OracleDB(SQLInterface):
    def __init__(self, config: dict, *args: Any, **kwargs: Any) -> None:
        super().__init__(config)
        self.as_dict = None
        self.connect(*args, **kwargs)

    def connect(self, *args: Any, **kwargs: Any) -> cx_Oracle.Connection:
        _config = {k: v for k, v in self.config.items()}
        for k, v in kwargs.items():
            _config[k] = v
        if self.ssh_config:
            self.inspect_config_for_sshtunnel(config=_config, host_key='host', port_key='port')
            _host, _port = self.set_sshtunnel(host=_config['host'], port=int(_config['port']))
            _config['host'] = _host
            _config['port'] = _port
        if 'dsn' in _config:
            pass
        elif 'host' in _config and 'port' in _config:
            tmp = {'host': _config.pop('host'), 'port': _config.pop('port')}
            if 'service_name' in _config:
                tmp['service_name'] = _config.pop('service_name')
            _config['dsn'] = cx_Oracle.makedsn(**tmp)
        else:
            raise ValueError('connection info is invalid')
        self.as_dict = False
        if 'as_dict' in _config:
            self.as_dict = self.eval_bool(_config.pop('as_dict'))
        self.session = cx_Oracle.connect(*args, **_config)
        self._closed = False
        return self.session

    def query(self, q: Union[str, list], commit: bool = False) -> Optional[list]:
        assert not self.closed(), "connection is closed"
        columns = None
        rows = None
        try:
            cursor = self.session.cursor()
            if type(q) == str:
                cursor.execute(q)
            elif type(q) == list:
                for qn in q:
                    cursor.execute(qn)
            if cursor.description:
                columns = tuple(c[0] for c in cursor.description)
                if self.as_dict:
                    cursor.rowfactory = partial(make_named_row, columns)
            rows = cursor.fetchall()
            cursor.close()
            if commit:
                self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e
        if self.as_dict:
            return rows
        else:
            if columns:
                return [columns] + list(rows)
            else:
                return None

    def close(self) -> None:
        self.close_sshtunnel()
        if not self.closed():
            self.session.close()
            self._closed = True
